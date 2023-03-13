import queue
import threading
import time
from enum import Enum
from typing import Optional

from py4j.protocol import Py4JJavaError
from pyflink.common import JobClient, JobStatus, Row
from pyflink.table import ResultKind, TableResult
from pyflink.table.table_result import CloseableIterator


class FetcherState(Enum):
    INITIALIZED = 0
    STARTED = 1
    WAITING_FOR_FIRST_RESULT = 2
    FETCHING_RESULTS = 3
    FINISHED = 4
    FAILED = 5
    CANCELLED = 6


class ResultStatus(Enum):
    NOT_AVAILABLE = 0
    EMPTY = 1
    AVAILABLE = 2


# Warning: ugly huck for a probable bug in Flink.
# (1) If execution_result.wait() timeout is too low, even if it is executed in a loop until no timeout is thrown,
# Flink may drop some results. If the timeout is high and there is no periodical polling for the first row,
# it seems that no results are dropped.
# (2) If an asyncio task is blocked for a long time, it prevents execution of other actions, e.g. action triggered
# by "Interrupt" button. This is why asyncio tasks should call "asyncio.sleep" periodically.
# Having (1) and (2) in mind, we have to delegate blocking action into a separate thread and check its status
# periodically in a non-blocking way.
# This thread will exit automatically once job is cancelled.


class ExecutionResultFetcher:
    def __init__(self,
                 execution_result: TableResult,
                 row_queue: queue.SimpleQueue[Optional[Row]],
                 first_row_polling_ms: int = 60 * 60 * 1000):
        self.execution_result = execution_result
        self.row_queue = row_queue
        self.first_row_polling_ms = first_row_polling_ms

        self.background_task = threading.Thread(target=self._background_task, args=(self.execution_result,))
        self.interrupted = False
        self.current_state = FetcherState.INITIALIZED

    def start(self) -> None:
        if self.current_state != FetcherState.INITIALIZED:
            raise ValueError("The fetcher cannot be started twice.")
        self.current_state = FetcherState.STARTED
        self.background_task.start()

    def interrupt(self) -> None:
        self.interrupted = True

    def wait(self, timeout: Optional[int] = None) -> None:
        self.background_task.join(timeout)

    def state(self) -> FetcherState:
        return self.current_state

    def result_status(self) -> ResultStatus:
        client = self.execution_result.get_job_client()
        # If client is None, then the result is returned immediately (e.g. metadata query).
        if client is None:
            return ResultStatus.AVAILABLE

        # The query does not return result immediately.
        job_status = self.get_job_status(client)
        if job_status is None:  # Job is not initialized yet
            return ResultStatus.NOT_AVAILABLE
        if job_status in [JobStatus.CREATED, JobStatus.RUNNING]:
            if self.state() in [FetcherState.STARTED, FetcherState.WAITING_FOR_FIRST_RESULT]:
                return ResultStatus.NOT_AVAILABLE
            elif self.state() in [FetcherState.FETCHING_RESULTS, FetcherState.FINISHED]:
                return ResultStatus.AVAILABLE
        else:  # job is finished/cancelled/failed
            time.sleep(2)
            # If job is done but the thread is still waiting for the first row, it means that the job
            # has no results at all.
            if self.state() in [FetcherState.WAITING_FOR_FIRST_RESULT]:
                return ResultStatus.EMPTY
            return ResultStatus.AVAILABLE
        raise RuntimeError(f"Unexpected state; job status={job_status}, fetcher state={self.state()}")

    def get_job_status(self, client: JobClient) -> JobStatus:
        try:
            return client.get_job_status().result()
        except Py4JJavaError as err:
            # hack for "local" mode
            if "MiniCluster is not yet running or has already been shut down" in str(err):
                return JobStatus.FINISHED
            else:
                raise err

    def _background_task(self, execution_result: TableResult) -> None:
        try:
            self.current_state = FetcherState.WAITING_FOR_FIRST_RESULT
            self._wait_for_the_first_result(execution_result)

            if self.interrupted:
                self.current_state = FetcherState.CANCELLED
                return

            self.current_state = FetcherState.FETCHING_RESULTS
            self._fetch_results(execution_result)
            self.current_state = FetcherState.FINISHED
        except Exception as err:  # noqa: B902
            self.current_state = FetcherState.FAILED
            raise err

    def _wait_for_the_first_result(self, execution_result: TableResult) -> None:
        while not self.interrupted:
            try:
                execution_result.wait(self.first_row_polling_ms)
            except Py4JJavaError as err:
                # consume timeout error
                if "java.util.concurrent.TimeoutException" in str(err):
                    continue
                # consume "job cancelled" error
                if "org.apache.flink.runtime.client.JobCancellationException: Job was cancelled" in str(err):
                    continue
                # rethrow any other error
                print("Exception while waiting for rows.", err)
                raise err
            except Exception as err:  # noqa: B902
                print("Exception while waiting for rows.", err)
                raise err

    def _fetch_results(self, execution_result: TableResult) -> None:
        result_kind = execution_result.get_result_kind()
        if result_kind == ResultKind.SUCCESS_WITH_CONTENT:
            with execution_result.collect() as results:
                try:
                    self._consume_results_iterator(results)
                except Py4JJavaError as e:
                    # consume "job cancelled" error or rethrow any other
                    if "org.apache.flink.runtime.client.JobCancellationException: Job was cancelled" not in str(e):
                        print("Exception while reading results.", e)
                        raise e
                except Exception as e:  # noqa: B902
                    print("Exception while reading results.", e)
                    raise e

    def _consume_results_iterator(self, results: CloseableIterator) -> None:
        for result in results:
            if self.interrupted:
                self.current_state = FetcherState.CANCELLED
                return
            self.row_queue.put(result)
        # None is a "poison pill"
        self.row_queue.put(None)
