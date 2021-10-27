from __future__ import print_function
from IPython.core.magic import (
    Magics, magics_class, line_magic,
    cell_magic, line_cell_magic
)

@magics_class
class Integrations(Magics):

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


# In order to actually use these magics, you must register them in Jupyter with a running IPython:
# %load_ext streamingcli.jupyter.integrations
#
# Then, to install it in local jypyter kernel, call:
#
# python -m pip install -e file:///Users/kalondar/projects/streaming-labs/streaming-cli
def load_ipython_extension(ipython):
    ipython.register_magics(Integrations)