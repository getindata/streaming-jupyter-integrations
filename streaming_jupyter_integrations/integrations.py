from __future__ import print_function
from IPython.core.magic import (
    Magics, magics_class, line_magic,
    cell_magic, line_cell_magic
)
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

from streamingcli.jupyter.display import display_execution_result
from streamingcli.jupyter.sql_utils import inline_sql_in_cell


@magics_class
class Integrations(Magics):

    def __init__(self, shell):
        super(Integrations, self).__init__(shell)
        self.s_env = StreamExecutionEnvironment.get_execution_environment()
        self.s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
        self.s_env.set_parallelism(1)

        self.st_env = StreamTableEnvironment.create(self.s_env,
                                                    environment_settings=EnvironmentSettings
                                                    .new_instance()
                                                    .in_streaming_mode()
                                                    .build())

    @cell_magic
    def deploy_as_flink_sql(self, line, cell):
        """Deploy cell SQL to ververica"""

        # TODO parse "line" to get profile_name ( expect string: "--profile local" )
        # TODO use deploy command

        # TODO return value
        return line, cell

    @cell_magic
    def execute_flink_sql(self, line, cell):
        """Just create, deploy and execute Flink job, get results and return them back to Jupyter"""

        # TODO parse "line" to get profile_name ( expect string: "--profile local" )
        # TODO use deploy command

        # TODO return value

        return line, cell

    @cell_magic
    def execute_flink_sql_local_cluster(self, line, cell):
        cell = inline_sql_in_cell(cell)
        execution_result = self.st_env.execute_sql(cell)
        display_execution_result(execution_result)


def load_ipython_extension(ipython):
    ipython.register_magics(Integrations)
