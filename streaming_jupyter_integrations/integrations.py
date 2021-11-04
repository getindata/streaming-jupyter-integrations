from __future__ import print_function

from IPython.core.magic import (
    Magics, magics_class, line_magic,
    cell_magic
)
from IPython.core.magic_arguments import argument, parse_argstring, magic_arguments
from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.java_gateway import get_gateway
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

from streamingcli.jupyter.display import display_execution_result
from streamingcli.jupyter.sql_utils import inline_sql_in_cell


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

    @cell_magic
    def flink_execute_sql(self, line, cell):
        cell = inline_sql_in_cell(cell)
        job_client = None
        try:
            execution_result = self.st_env.execute_sql(cell)
            job_client = execution_result.get_job_client()
            execution_result.wait()
            print('Execution successful')
        except KeyboardInterrupt:
            if job_client is not None:
                print('Job cancelled ' + str(job_client.get_job_id()))
                job_client.cancel().result()

    @cell_magic
    def flink_query_sql(self, line, cell):
        cell = inline_sql_in_cell(cell)
        try:
            execution_result = self.st_env.execute_sql(cell)
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


def load_ipython_extension(ipython):
    ipython.register_magics(Integrations)
