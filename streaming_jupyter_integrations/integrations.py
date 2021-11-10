from __future__ import print_function

import signal

from IPython.core.magic import (
    Magics, magics_class, line_magic,
    cell_magic
)
from IPython.core.magic_arguments import argument, parse_argstring, magic_arguments
from py4j.protocol import Py4JJavaError
from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.java_gateway import get_gateway
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

from streamingcli.jupyter.display import display_execution_result
from streamingcli.jupyter.reflection import get_method_names_for
from streamingcli.jupyter.sql_syntax_highlighting import SQLSyntaxHighlighting
from streamingcli.jupyter.sql_utils import inline_sql_in_cell
from streamingcli.jupyter.variable_substitution import CellContentFormatter


@magics_class
class Integrations(Magics):

    def __init__(self, shell):
        super(Integrations, self).__init__(shell)
        conf = Configuration()
        conf.set_integer("rest.port", 8099)
        conf.set_integer("parallelism.default", 1)
        self.s_env = StreamExecutionEnvironment(
            get_gateway().jvm.org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
            .createLocalEnvironmentWithWebUI(conf._j_configuration))
        self.st_env = StreamTableEnvironment.create(stream_execution_environment=self.s_env,
                                                    environment_settings=EnvironmentSettings
                                                    .new_instance()
                                                    .in_streaming_mode()
                                                    .build())
        self.interrupted = False
        self.polling_ms = 1000
        Integrations.__enable_sql_syntax_highlighting()

    @cell_magic
    def flink_execute_sql(self, line, cell):
        # override SIGINT handlers so that they are not propagated
        # to flink java processes. A copy of the original handler is saved
        # so that we can restore it later on.
        # Side note: boolean assignment is atomic in python.
        self.interrupted = False
        original_sigint = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, self.__interrupt_execute)

        try:
            enriched_cell = CellContentFormatter(cell, self.shell.user_ns).substitute_user_variables()
            self.__internal_execute_sql(line, enriched_cell)
        finally:
            signal.signal(signal.SIGINT, original_sigint)

    # a workaround for https://issues.apache.org/jira/browse/FLINK-23020
    def __internal_execute_sql(self, line, cell):
        cell = inline_sql_in_cell(cell)
        execution_result = self.st_env.execute_sql(cell)
        successful_execution_msg = 'Execution successful'

        # active polling
        while not self.interrupted:
            try:
                execution_result.wait(self.polling_ms)
                # if finished then return early even if the user interrupts after this
                # the actual invocation has already finished
                print(successful_execution_msg)
                return
            except Py4JJavaError as err:
                # consume timeout error or rethrow any other
                if 'java.util.concurrent.TimeoutException' not in str(err.java_exception):
                    raise err

        if self.interrupted:
            job_client = execution_result.get_job_client()
            if job_client is not None:
                print(f'Job cancelled {job_client.get_job_id()}')
                job_client.cancel().result()
            else:
                # interrupted and executed a stmt without a proper job (see the underlying execute_sql call)
                print('Job interrupted')
            # in either case return early
            return

        # usual happy path
        print(successful_execution_msg)

    @cell_magic
    def flink_query_sql(self, line, cell):
        cell = inline_sql_in_cell(cell)
        enriched_cell = CellContentFormatter(cell, self.shell.user_ns).substitute_user_variables()
        try:
            execution_result = self.st_env.execute_sql(enriched_cell)
            display_execution_result(execution_result)
        except KeyboardInterrupt:
            print('Query cancelled')

    @line_magic
    @magic_arguments()
    @argument('-n', '--function_name', type=str,
              help='A function name which will be used in SQL eg. MY_COUNTER',
              required=True)
    @argument('-u', '--object_name', type=str,
              help='A created udf object eg. my_counter',
              required=True)
    def flink_register_function(self, line):
        args = parse_argstring(self.flink_register_function, line)
        shell = self.shell
        function_name = args.function_name
        udf_obj = shell.user_ns[args.object_name]
        self.st_env.create_temporary_function(function_name, udf_obj)
        print(f'Function {function_name} registered')

    @staticmethod
    def __enable_sql_syntax_highlighting():
        methods_decorated_with_cell_magic = get_method_names_for(Integrations, 'cell_magic')
        sql_highlighting = SQLSyntaxHighlighting(methods_decorated_with_cell_magic)
        sql_highlighting.add_syntax_highlighting_js()

    def __interrupt_execute(self, *args):
        self.interrupted = True


def load_ipython_extension(ipython):
    ipython.register_magics(Integrations)
