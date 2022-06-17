from __future__ import print_function

import asyncio
import os
import signal
from functools import wraps
from typing import Any, Callable, Dict, Iterable, Tuple

import nest_asyncio
import pandas as pd
import sqlparse
import yaml
from IPython import display
from IPython.core.display import display as core_display
from IPython.core.magic import Magics, cell_magic, line_magic, magics_class
from IPython.core.magic_arguments import (argument, magic_arguments,
                                          parse_argstring)
from ipywidgets import IntText
from jupyter_core.paths import jupyter_config_dir
from py4j.protocol import Py4JJavaError
from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.java_gateway import get_gateway
from pyflink.table import (EnvironmentSettings, ResultKind,
                           StreamTableEnvironment, TableResult)

from .config_utils import load_config_file
from .deployment_bar import DeploymentBar
from .display import pyflink_result_kind_to_string
from .jar_handler import JarHandler
from .reflection import get_method_names_for
from .sql_syntax_highlighting import SQLSyntaxHighlighting
from .sql_utils import inline_sql_in_cell, is_dml, is_query
from .variable_substitution import CellContentFormatter


@magics_class
class Integrations(Magics):
    def __init__(self, shell: Any):
        super(Integrations, self).__init__(shell)
        print(
            "Set env variable JAVA_TOOL_OPTIONS="
            "'--add-opens=java.base/java.util=ALL-UNNAMED "
            "--add-opens=java.base/java.lang=ALL-UNNAMED'"
        )
        os.environ["JAVA_TOOL_OPTIONS"] = (
            "--add-opens=java.base/java.util=ALL-UNNAMED "
            "--add-opens=java.base/java.lang=ALL-UNNAMED"
        )
        global_conf = self.__read_global_config()
        conf = self.__create_configuration_from_dict(global_conf)
        conf.set_integer("rest.port", 8099)
        conf.set_integer("parallelism.default", 1)
        self.s_env = StreamExecutionEnvironment(
            get_gateway().jvm.org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                conf._j_configuration
            )
        )
        self.st_env = StreamTableEnvironment.create(
            stream_execution_environment=self.s_env,
            environment_settings=EnvironmentSettings.new_instance()
                .in_streaming_mode()
                .build(),
        )
        self.interrupted = False
        self.polling_ms = 100
        # 20ms
        self.async_wait_s = 2e-2
        Integrations.__enable_sql_syntax_highlighting()
        self.deployment_bar = DeploymentBar(interrupt_callback=self.__interrupt_execute)
        # Indicates whether a job is executing on the Flink cluster in the background
        self.background_execution_in_progress = False
        self.jar_handler = JarHandler(project_root_dir=os.getcwd())
        # Enables nesting blocking async tasks
        nest_asyncio.apply()

    @line_magic
    @magic_arguments()
    @argument(
        "-p", "--path", type=str, help="A path to a local config file", required=True
    )
    def load_config_file(self, line: str) -> None:
        args = parse_argstring(self.load_config_file, line)
        path = args.path
        loaded_variables = load_config_file(path)
        self.shell.user_ns.update(loaded_variables)
        print("Config file loaded")

    def __interrupt_signal_decorator(method: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(method)
        def _impl(self: 'Integrations', *method_args: Tuple[Any], **method_kwargs: Dict[Any, Any]) -> None:
            self.interrupted = False
            # override SIGINT handlers so that they are not propagated
            # to flink java processes. A copy of the original handler is saved
            # so that we can restore it later on.
            # Side note: boolean assignment is atomic in python.
            original_sigint = signal.getsignal(signal.SIGINT)
            signal.signal(signal.SIGINT, self.__interrupt_execute)
            try:
                method(self, *method_args, **method_kwargs)
            finally:
                signal.signal(signal.SIGINT, original_sigint)
        return _impl

    @line_magic
    @magic_arguments()
    @argument(
        "-p", "--path", type=str, help="A path to a local config file", required=True
    )
    def flink_execute_sql_file(self, line: str) -> None:
        if self.background_execution_in_progress:
            self.__retract_user_as_something_is_executing_in_background()
            return

        args = parse_argstring(self.flink_execute_sql_file, line)
        path = args.path
        with open(path, "r") as f:
            statements = map(lambda s: self.__enrich_cell(s.rstrip(';')), sqlparse.split(f.read()))

        self.background_execution_in_progress = True
        self.deployment_bar.show_deployment_bar()
        try:
            self.__flink_execute_sql_file_internal(statements)
        finally:
            self.background_execution_in_progress = False

    @__interrupt_signal_decorator
    def __flink_execute_sql_file_internal(self, statements: Iterable[str]) -> None:
        for stmt in statements:
            if self.interrupted:
                break
            task = self.__internal_execute_sql(stmt)
            asyncio.run(task)

    @cell_magic
    def flink_execute_sql(self, line: str, cell: str) -> None:
        if self.background_execution_in_progress:
            self.__retract_user_as_something_is_executing_in_background()
            return

        stmt = self.__enrich_cell(cell)
        self.__flink_execute_sql_internal(stmt)

    @__interrupt_signal_decorator
    def __flink_execute_sql_internal(self, stmt: str) -> None:
        task = self.__internal_execute_sql(stmt)
        if is_dml(stmt) or is_query(stmt):
            print(
                "This job runs in a background, please either wait or interrupt its execution before continuing"
            )
            self.background_execution_in_progress = True
            self.deployment_bar.show_deployment_bar()
            asyncio.create_task(task).add_done_callback(self.__handle_done)
        else:
            # if not DML or SELECT then the operation is synchronous
            # synchronous operations are not interactive, one cannot cancel them
            # and hence showing the deployment bar does not make sense
            asyncio.run(task)

    # a workaround for https://issues.apache.org/jira/browse/FLINK-23020
    async def __internal_execute_sql(self, stmt: str) -> None:
        print("Job starting...")
        execution_result = self.st_env.execute_sql(stmt)
        print("Job started")
        successful_execution_msg = "Execution successful"

        # active polling
        while not self.interrupted:
            try:
                # Explicit await is needed to unblock the main thread to pick up other tasks.
                # In Jupyter's main execution pool there is only one worker thread.
                await asyncio.sleep(self.async_wait_s)
                execution_result.wait(self.polling_ms)
                if is_query(stmt):
                    # if a select query has been executing then `wait` returns as soon as the first
                    # row is available. To display the results
                    print("Pulling query results...")
                    await self.display_execution_result(execution_result)
                    return
                else:
                    # if finished then return early even if the user interrupts after this
                    # the actual invocation has already finished
                    print(successful_execution_msg)
                    return
            except Py4JJavaError as err:
                # consume timeout error or rethrow any other
                if "java.util.concurrent.TimeoutException" not in str(
                        err.java_exception
                ):
                    raise err

        if self.interrupted:
            job_client = execution_result.get_job_client()
            if job_client is not None:
                print(f"Job cancelled {job_client.get_job_id()}")
                job_client.cancel().result()
            else:
                # interrupted and executed a stmt without a proper job (see the underlying execute_sql call)
                print("Job interrupted")
            # in either case return early
            return

        # usual happy path
        print(successful_execution_msg)

    async def display_execution_result(self, execution_result: TableResult) -> pd.DataFrame:
        """
        Displays the execution result and returns a dataframe containing all the results.
        Display is done in a stream-like fashion displaying the results as they come.
        """

        columns = execution_result.get_table_schema().get_field_names()
        df = pd.DataFrame(columns=columns)
        result_kind = execution_result.get_result_kind()

        if result_kind == ResultKind.SUCCESS_WITH_CONTENT:
            with execution_result.collect() as results:
                print(
                    "Results will be pulled from the job. You can interrupt any time to show partial results."
                )
                print("Execution result will bind to `execution_result` variable.")
                rows_counter = IntText(value=0, description="Loaded rows: ")
                core_display(rows_counter)
                for result in results:
                    # Explicit await for the same reason as in `__internal_execute_sql`
                    await asyncio.sleep(self.async_wait_s)
                    res = list(result)
                    a_series = pd.Series(res, index=df.columns)
                    df = df.append(a_series, ignore_index=True)
                    rows_counter.value += 1

                    if self.interrupted:
                        print("Query interrupted")
                        break
        else:
            series = pd.Series(
                [pyflink_result_kind_to_string(result_kind)], index=df.columns
            )
            df = df.append(series, ignore_index=True)

        display.display(df)
        self.shell.user_ns["execution_result"] = df

        return df

    @line_magic
    @magic_arguments()
    @argument(
        "-p",
        "--local_path",
        type=str,
        help="A path to a local jar to include in the deployment",
        required=False,
    )
    @argument(
        "-r",
        "--remote_path",
        type=str,
        help="A path to a remote jar to include in the deployment",
        required=False,
    )
    def flink_register_jar(self, line: str) -> None:
        args = parse_argstring(self.flink_register_jar, line)
        local_path = args.local_path
        remote_path = args.remote_path
        classpath_to_add = None
        if local_path and remote_path:
            raise ValueError(
                "Cannot specify both local and remote path, please use two consecutive magics."
            )

        if local_path:
            # Locally copy the file from the source to a local jar storage.
            # This is done to ensure that we can copy the local jars to the remote flink cluster.
            # (Jupyter executes in memory and can definitely access the file,
            # however, the deployment might not be able to)
            classpath_to_add = self.jar_handler.local_copy(local_path)
        elif remote_path:
            # Remotely just copy the file to a local folder and run the same.
            # The decision is to copy file rather than keep it as an URL - the deployment might
            # be cut off the internet completely so it does make sense. If in the future
            # it is deemed feasible to keep the URL and let flink pull the jars then it can be extended.
            classpath_to_add = self.jar_handler.remote_copy(remote_path)
        else:
            raise ValueError(
                "Please specify either a local or remote path, use `%flink_register_jar?` for help."
            )

        pipeline_classpaths = "pipeline.classpaths"
        current_classpaths = (
            self.st_env.get_config()
                .get_configuration()
                .get_string(pipeline_classpaths, "")
        )
        new_classpath = (
            f"{current_classpaths};{classpath_to_add}"
            if len(current_classpaths) > 0
            else classpath_to_add
        )
        self.st_env.get_config().get_configuration().set_string(
            pipeline_classpaths, new_classpath
        )
        print(f"Jar {classpath_to_add} registered")

    @line_magic
    @magic_arguments()
    @argument(
        "-n",
        "--function_name",
        type=str,
        help="A function name which will be used in SQL eg. MY_COUNTER",
        required=True,
    )
    @argument(
        "-u",
        "--object_name",
        type=str,
        help="A created udf object eg. my_counter",
        required=True,
    )
    @argument(
        "-l",
        "--language",
        type=str,
        help="A language that the UDF was written in, for now we support python and java. Defaults to python.",
        default="python",
        required=False,
    )
    def flink_register_function(self, line: str) -> None:
        args = parse_argstring(self.flink_register_function, line)
        shell = self.shell
        function_name = args.function_name
        language = args.language if args.language else "python"
        if language == "python":
            udf_obj = shell.user_ns[args.object_name]
            self.st_env.create_temporary_function(function_name, udf_obj)
        elif language == "java":
            self.st_env.create_java_temporary_function(function_name, args.object_name)
        else:
            raise ValueError("Supported languages are: java, python.")
        print(f"Function {function_name} registered [{language}]")

    @staticmethod
    def __enable_sql_syntax_highlighting() -> None:
        methods_decorated_with_cell_magic = get_method_names_for(
            Integrations, "cell_magic"
        )
        sql_highlighting = SQLSyntaxHighlighting(
            methods_decorated_with_cell_magic, jupyter_config_dir()
        )
        sql_highlighting.add_syntax_highlighting_js()

    def __interrupt_execute(self, *args: Any) -> None:
        print("Job termination in progress...")
        self.interrupted = True

    def __handle_done(self, fut: Any) -> None:
        self.background_execution_in_progress = False
        print("Execution done")
        # https://stackoverflow.com/questions/48161387/python-how-to-print-the-stacktrace-of-an-exception-object-without-a-currently
        # will raise an exception to the main thread
        if fut.exception():
            fut.result()

    def __enrich_cell(self, cell: str) -> str:
        enriched_cell = CellContentFormatter(
            cell, self.shell.user_ns
        ).substitute_user_variables()
        joined_cell = inline_sql_in_cell(enriched_cell)
        return joined_cell

    @staticmethod
    def __retract_user_as_something_is_executing_in_background() -> None:
        print("Please wait for the previously submitted task to finish or cancel it.")

    @staticmethod
    def __read_global_config() -> Dict[str, Any]:
        if "FLINK_HOME" in os.environ:
            with open(os.path.join(os.environ["FLINK_HOME"], "conf", "flink-conf.yaml"), 'r') as stream:
                try:
                    parsed_yaml = yaml.safe_load(stream)
                    return parsed_yaml
                except yaml.YAMLError as exc:
                    print(exc)
        else:
            print("FLINK_HOME environment variable is not set, reading flink-conf skipped")
        return {}

    @staticmethod
    def __create_configuration_from_dict(new_values: Dict[str, Any]) -> Configuration:
        configuration = Configuration()
        for key, value in new_values.items():
            if type(value) is str:
                configuration.set_string(key, value)
            elif type(value) is int:
                configuration.set_integer(key, value)
            elif type(value) is float:
                configuration.set_float(key, value)
            elif type(value) is bool:
                configuration.set_boolean(key, value)
            elif type(value) is bytearray:
                configuration.set_bytearray(key, value)
            else:
                print(f"No setter available for {key}")

        return configuration


def load_ipython_extension(ipython: Any) -> None:
    ipython.register_magics(Integrations)
