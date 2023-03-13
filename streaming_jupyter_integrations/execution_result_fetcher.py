import queue
import threading
from enum import Enum
from typing import Optional

from py4j.protocol import Py4JJavaError
from pyflink.table import ResultKind, TableResult
from pyflink.table.table_result import CloseableIterator


class FetcherState(Enum):
    INITIALIZED = 0
    STARTED = 1
    FETCHING_RESULTS = 2
    FINISHED = 3
    FAILED = 4
    CANCELLED = 5


class ResultStatus(Enum):
    NOT_AVAILABLE = 0
    AVAILABLE = 1


class ExecutionResultFetcher:
    def __init__(self,
                 execution_result: TableResult,
                 row_queue: queue.SimpleQueue):
        self.execution_result = execution_result
        self.row_queue = row_queue

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
        if self.state() in [FetcherState.INITIALIZED, FetcherState.STARTED]:
            return ResultStatus.NOT_AVAILABLE
        elif self.state() in [FetcherState.FETCHING_RESULTS, FetcherState.FINISHED]:
            return ResultStatus.AVAILABLE
        raise RuntimeError(f"Unexpected state, fetcher state={self.state()}")

    def _background_task(self, execution_result: TableResult) -> None:
        try:
            self.current_state = FetcherState.FETCHING_RESULTS
            self._fetch_results(execution_result)
            self.current_state = FetcherState.FINISHED
        except Exception as err:  # noqa: B902
            self.current_state = FetcherState.FAILED
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
            self.row_queue.put(result)
            if self.interrupted:
                self.current_state = FetcherState.CANCELLED
                return
        # None is a "poison pill"
        self.row_queue.put(None)
