import queue
import threading
from typing import Optional

from py4j.protocol import Py4JJavaError
from pyflink.common import Row
from pyflink.table import ResultKind, TableResult
from pyflink.table.table_result import CloseableIterator


class ExecutionResultFetcher:
    def __init__(self,
                 execution_result: TableResult,
                 row_queue: queue.SimpleQueue[Optional[Row]]):
        self.execution_result = execution_result
        self.row_queue = row_queue

        self.background_task = threading.Thread(target=self._background_task, args=(self.execution_result,))
        self.interrupted = False

    def start(self) -> None:
        self.background_task.start()

    def interrupt(self) -> None:
        self.interrupted = True

    def wait(self, timeout: Optional[int] = None) -> None:
        self.background_task.join(timeout)

    def _background_task(self, execution_result: TableResult) -> None:
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
                return
        # None is a "poison pill"
        self.row_queue.put(None)
