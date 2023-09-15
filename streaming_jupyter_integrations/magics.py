from __future__ import annotations, print_function

import asyncio
import os
import pathlib
import queue
import signal
import subprocess
import sys
import threading
import time
import warnings
from functools import wraps
from typing import Any, Callable, Dict, Iterable, Optional, Tuple, Union, cast

import nest_asyncio
import sqlparse
import yaml
from IPython import display
from IPython.core.display import display as core_display
from IPython.core.magic import Magics, cell_magic, line_magic, magics_class
from IPython.core.magic_arguments import (argument, magic_arguments,
                                          parse_argstring)
from ipywidgets import IntText
from jupyter_core.paths import jupyter_config_dir
from py4j.java_collections import JavaArray
from py4j.protocol import Py4JJavaError
from pyflink.common import Configuration, JobClient, JobStatus
from pyflink.common.types import Row
from pyflink.datastream import DataStream, StreamExecutionEnvironment
from pyflink.java_gateway import get_gateway
from pyflink.table import (EnvironmentSettings, ResultKind,
                           StreamTableEnvironment, Table, TableResult)

from .cast_utils import cast_timestamp_ltz_to_string
from .config_utils import load_config_file, read_flink_config_file
from .deployment_bar import DeploymentBar
from .display import pyflink_result_kind_to_string
from .jar_handler import JarHandler
from .reflection import get_method_names_for
from .schema_view import (IPyTreeSchemaBuilder, JsonTreeSchemaBuilder,
                          SchemaLoader)
from .sql_syntax_highlighting import SQLSyntaxHighlighting
from .sql_utils import (inline_sql_in_cell, is_dml, is_dql, is_metadata_query,
                        is_query)
from .variable_substitution import CellContentFormatter
from .yarn import find_session_jm_address

# https://stackoverflow.com/questions/15777951/how-to-suppress-pandas-future-warning?rq=4
# Warning to suppress:
#   /opt/conda/lib/python3.8/site-packages/streaming_jupyter_integrations/magics.py:561: FutureWarning:
#   In a future version, object-dtype columns with all-bool values will not be included in reductions with
#   bool_only=True. Explicitly cast to bool dtype instead. df = pd.concat([df, pd.DataFrame.from_records([a_series])])
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd  # noqa: E402


@magics_class
class Integrations(Magics):

    def __init__(self, shell: Any):
        super(Integrations, self).__init__(shell)
        self._secrets: Dict[str, str] = {}
        self._load_secrets_from_scli_config()
        self._set_java_options()
        self.jar_handler = JarHandler(project_root_dir=os.getcwd())
        self.interrupted = False
        self.first_row_polling_ms = 60 * 60 * 1000  # 1h
        # 20ms
        self.async_wait_s = 2e-2
        Integrations.__enable_sql_syntax_highlighting()
        self.deployment_bar = DeploymentBar(interrupt_callback=self.__interrupt_execute)
        # Indicates whether a job is executing on the Flink cluster in the background
        self.background_execution_in_progress = False
        # Enables nesting blocking async tasks
        nest_asyncio.apply()

    def _set_java_options(self) -> None:
        if not self._is_java_8():
            print(
                "Set env variable JAVA_TOOL_OPTIONS="
                "'--add-opens=java.base/java.util=ALL-UNNAMED "
                "--add-opens=java.base/java.lang=ALL-UNNAMED'"
            )
            os.environ["JAVA_TOOL_OPTIONS"] = (
                "--add-opens=java.base/java.util=ALL-UNNAMED "
                "--add-opens=java.base/java.lang=ALL-UNNAMED"
            )

    @staticmethod
    def _is_java_8() -> bool:
        java_version_out = subprocess.check_output(['java', '-version'], stderr=subprocess.STDOUT).decode('utf-8')
        return "1.8.0_" in java_version_out.splitlines()[0]

    @line_magic
    @magic_arguments()
    @argument("-e", "--execution-mode", type=str, help="Flink execution mode", required=False,
              default="streaming")
    @argument("-t", "--execution-target", type=str, help="The target on which queries will be executed", required=False,
              default="local")
    @argument("-lp", "--local-port", type=int, help="Port of the local JobManager", required=False, default=8099)
    @argument("-rh", "--remote-hostname", type=str, help="Hostname of the remote JobManager", required=False)
    @argument("-rp", "--remote-port", type=int, help="Port of the remote JobManager", required=False)
    @argument("-rmh", "--resource-manager-hostname", type=str, help="YARN Resource Manager hostname", required=False)
    @argument("-rmp", "--resource-manager-port", type=int, help="YARN Resource Manager port", required=False)
    @argument("-yid", "--yarn-application-id", type=str, help="Flink Session Cluster applicationId", required=False)
    def flink_connect(self, line: str) -> None:
        args = parse_argstring(self.flink_connect, line)
        try:
            self._flink_connect(args)
        except Exception as e:  # noqa: B902
            print(e, file=sys.stderr)

    def _flink_connect(self, args: Any) -> None:
        execution_mode = args.execution_mode
        execution_target = args.execution_target

        if execution_target == "local":
            self._flink_connect_local(args.local_port)
        elif execution_target == "remote":
            if not args.remote_hostname or not args.remote_port:
                raise ValueError("Remote execution target requires --remote-hostname and --remote-port parameters.")
            self._flink_connect_remote(args.remote_hostname, args.remote_port)
        elif execution_target == "yarn-session":
            self._flink_connect_yarn_session(args.resource_manager_hostname, args.resource_manager_port,
                                             args.yarn_application_id)
        else:
            raise ValueError(
                f"Unknown execution mode. Expected 'local', 'remote' or 'yarn-session', actual '{execution_target}'.")

        self._set_table_env(execution_mode)
        self.__load_plugins()
        self.__flink_execute_sql_file("init.sql", display_row_kind=False)
        self._initialize_ds_exec_variables()
        print(f"{execution_target} environment has been created.")

    def _initialize_ds_exec_variables(self) -> None:
        # Expose only execution environment references.
        self.__ds_exec_globals: Dict[str, Any] = {}
        self.__ds_exec_locals = {
            "table_env": self.st_env,
            "stream_env": self.s_env
        }

    def _flink_connect_local(self, port: int) -> None:
        global_conf = read_flink_config_file()
        conf = self.__create_configuration_from_dict(global_conf)
        conf.set_integer("rest.port", port)
        conf.set_integer("parallelism.default", 1)
        self.s_env = StreamExecutionEnvironment(
            get_gateway().jvm.org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(  # noqa: E501
                conf._j_configuration
            )
        )

    def _flink_connect_remote(self, hostname: str, port: int) -> None:
        global_conf = read_flink_config_file()
        conf = self.__create_configuration_from_dict(global_conf)
        gateway = get_gateway()
        pipeline_jars_list = self._get_pipeline_jars(conf)
        self.s_env = StreamExecutionEnvironment(
            gateway.jvm.org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.createRemoteEnvironment(
                hostname, port, conf._j_configuration, pipeline_jars_list
            )
        )

    @staticmethod
    def _get_pipeline_jars(conf: Configuration) -> JavaArray:
        # When calling StreamExecutionEnvironment.createRemoteEnvironment() "pipeline.jars" property is overwritten
        # with paths of jars specified as "String jarFiles" parameter. In consequence, we need to parse the property
        # and pass the paths as "jarFiles" parameter.
        # What is more, jarFiles are resolved incorrectly if the paths contain "file://" schema
        # (org.apache.flink.api.java.RemoteEnvironmentConfigUtils#getJarFiles). On the other hand, jars specified in
        # "pipeline.jars" have to contain the schema. Therefore, we need to remove the schema on our own.
        gateway = get_gateway()
        pipeline_jars = list(filter(lambda p: p, conf.get_string("pipeline.jars", "").split(";")))
        result = gateway.new_array(gateway.jvm.String, len(pipeline_jars))
        for i in range(len(pipeline_jars)):
            result[i] = pipeline_jars[i].replace("file://", "")
        return result

    def _flink_connect_yarn_session(self, rm_hostname: str, rm_port: int, yarn_application_id: str) -> None:
        jm_hostname, jm_port = find_session_jm_address(rm_hostname, rm_port, yarn_application_id)
        self._flink_connect_remote(jm_hostname, jm_port)

    def _set_table_env(self, execution_mode: str) -> None:
        if execution_mode == "batch":
            self.st_env = StreamTableEnvironment.create(
                stream_execution_environment=self.s_env,
                environment_settings=EnvironmentSettings.new_instance().in_batch_mode().build(),
            )
        else:
            self.st_env = StreamTableEnvironment.create(
                stream_execution_environment=self.s_env,
                environment_settings=EnvironmentSettings.new_instance().in_streaming_mode().build(),
            )

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

    @line_magic
    @magic_arguments()
    @argument("-p", "--path", type=str, help="A path to a secret file", required=True)
    @argument("varname", type=str, help="A name of the variable that will hold the secret")
    def load_secret_file(self, line: str) -> None:
        args = parse_argstring(self.load_secret_file, line)
        var_name, path = args.varname, args.path
        self._load_secret_from_file(var_name, path)

    def _load_secret_from_file(self, var_name: str, filepath: str) -> None:
        with open(filepath, "r") as secret_file:
            secret_value = secret_file.read().rstrip()
        if self._secrets is None:
            self._secrets = {}
        self._secrets[var_name] = secret_value
        print(f"Content of the secret file '{filepath}' loaded into '{var_name}' variable")

    # this references `.streaming_config.yml` file that is a part of projects
    # generated by https://github.com/getindata/streaming-cli/
    def _load_secrets_from_scli_config(self) -> None:
        project_src_dir = pathlib.Path.cwd()
        if project_src_dir.name == 'src':
            project_src_dir = project_src_dir.parent
        config_file_path = project_src_dir / ".streaming_config.yml"
        if not config_file_path.exists():
            print(f"Could not find scli config file at '{config_file_path}'. Will not load any secrets.")
            return
        with open(config_file_path, "r") as config_file:
            try:
                config = cast(Dict[str, Any], yaml.safe_load(config_file))
            except yaml.YAMLError:
                print(f"Cannot read scli config from {config_file_path}. It is not a valid YAML file.")
                return
        secrets_list = cast(Dict[str, str], config.get("secrets", {}))
        for var_name, filepath in secrets_list.items():
            self._load_secret_from_file(var_name, filepath)

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
    @argument("--display-row-kind", help="Whether result row kind should be displayed", action="store_true")
    def flink_execute_sql_file(self, line: str) -> None:
        args = parse_argstring(self.flink_execute_sql_file, line)
        path = args.path
        if os.path.exists(path):
            self.__flink_execute_sql_file(path, args.display_row_kind)
        else:
            print("File {} not found".format(path))

    @__interrupt_signal_decorator
    def __flink_execute_sql_file_internal(self, statements: Iterable[str], display_row_kind: bool) -> None:
        for stmt in statements:
            if self.interrupted:
                break
            task = self.__internal_execute_sql(stmt, display_row_kind)
            asyncio.run(task)

    @cell_magic
    @magic_arguments()
    @argument("--display-row-kind", help="Whether result row kind should be displayed", action="store_true")
    @argument("--parallelism", "-p", type=int, help="Flink parallelism to use when running the code", required=False,
              default=1)
    def flink_execute(self, line: str, cell: str) -> None:
        args = parse_argstring(self.flink_execute, line)
        self.s_env.set_parallelism(args.parallelism)
        if self.background_execution_in_progress:
            self.__retract_user_as_something_is_executing_in_background()
            return
        self.___flink_execute_internal(cell, args.display_row_kind)

    @__interrupt_signal_decorator
    def ___flink_execute_internal(self, stmt: str, display_row_kind: bool) -> None:
        task = self.__flink_execute_internal(stmt, display_row_kind)
        print("This job runs in a background, please either wait or interrupt its execution before continuing")
        self.background_execution_in_progress = True
        self.deployment_bar.show_deployment_bar()
        asyncio.create_task(task).add_done_callback(self.__handle_done)

    async def __flink_execute_internal(self, stmt: str, display_row_kind: bool) -> None:
        print("Job starting...")
        # execution_output is a special variable, clear it before running the code.
        if "execution_output" in self.__ds_exec_locals:
            del self.__ds_exec_locals["execution_output"]
        exec(stmt, self.__ds_exec_globals, self.__ds_exec_locals)
        execution_output = self.__ds_exec_locals.get("execution_output")
        print("Job started")
        if execution_output is None:
            # If execution_output is undefined, it means that the cell contains only definitions that will be used
            # in the following cells. Nothing to display.
            return
        if isinstance(execution_output, Table):
            await self.__pull_results(execution_output.execute(), display_row_kind, True)
        elif isinstance(execution_output, TableResult):
            await self.__pull_results(execution_output, display_row_kind, True)
        elif isinstance(execution_output, DataStream):
            execution_output = self.st_env.from_data_stream(execution_output).execute()
            await self.__pull_results(execution_output, display_row_kind, True)
        else:
            print(f"Unexpected type of 'execution_output'. Actual {type(execution_output)}, expected either "
                  "TableResult or DataStream.", file=sys.stderr)

    @cell_magic
    @magic_arguments()
    @argument("--parallelism", "-p", type=int, help="Flink parallelism to use when running the SQL", required=False,
              default=1)
    def flink_execute_sql_set(self, line: str, cell: str) -> None:
        args = parse_argstring(self.flink_execute_sql_set, line)
        self.s_env.set_parallelism(args.parallelism)

        if self.background_execution_in_progress:
            self.__retract_user_as_something_is_executing_in_background()
            return

        stmt = self.__enrich_cell(cell)
        self.___flink_execute_sql_set_internal(stmt)

    @__interrupt_signal_decorator
    def ___flink_execute_sql_set_internal(self, stmt: str) -> None:
        task = self.__flink_execute_sql_set_internal(stmt)
        print("This job runs in a background, please either wait or interrupt its execution before continuing")
        self.background_execution_in_progress = True
        self.deployment_bar.show_deployment_bar()
        asyncio.create_task(task).add_done_callback(self.__handle_done)

    async def __flink_execute_sql_set_internal(self, stmt: str) -> None:
        print("Job starting...")

        stmt_set = self.st_env.create_statement_set()
        for insert in sqlparse.split(stmt):
            stmt_set.add_insert_sql(insert)
        execution_output = stmt_set.execute()
        print("Job started")
        await self.__pull_results(execution_output, False, False)

    @cell_magic
    @magic_arguments()
    @argument("--display-row-kind", help="Whether result row kind should be displayed", action="store_true")
    @argument("--parallelism", "-p", type=int, help="Flink parallelism to use when running the SQL", required=False,
              default=1)
    def flink_execute_sql(self, line: str, cell: str) -> None:
        args = parse_argstring(self.flink_execute_sql, line)
        self.s_env.set_parallelism(args.parallelism)

        if self.background_execution_in_progress:
            self.__retract_user_as_something_is_executing_in_background()
            return

        stmt = self.__enrich_cell(cell)
        self.__flink_execute_sql_internal(stmt, args.display_row_kind)

    @__interrupt_signal_decorator
    def __flink_execute_sql_internal(self, stmt: str, display_row_kind: bool) -> None:
        task = self.__internal_execute_sql(stmt, display_row_kind)
        if is_dml(stmt) or is_dql(stmt):
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
    async def __internal_execute_sql(self, stmt: str, display_row_kind: bool) -> None:
        print("Job starting...")
        # Workaround - Python API does not work well with TIMESTAMP_LTZ type. If the output table contains the field,
        # cast it to string first.
        if is_query(stmt):
            execution_table = self.st_env.sql_query(stmt)
            execution_result = cast_timestamp_ltz_to_string(self.st_env, execution_table).execute()
        else:
            execution_result = self.st_env.execute_sql(stmt)
        print("Job started")
        # Pandas lib truncates view if the number of results exceeds the limit. The same applies to column width.
        # If the query shows metadata, e.g. list of tables or list of columns, then no limit is applied.
        pd_display_options = {
            "display.max_rows": None if is_metadata_query(stmt) else 100,
            "display.max_colwidth": None if is_metadata_query(stmt) else 100,
        }
        await self.__pull_results(execution_result, display_row_kind, is_dql(stmt), pd_display_options)

    # Warning: ugly huck for a probable bug in Flink.
    # (1) If execution_result.wait() timeout is too low, even if it is executed in a loop until no timeout is thrown,
    # Flink may drop some results. If the timeout is high and there is no periodical polling for the first row,
    # it seems that no results are dropped.
    # (2) If an asyncio task is blocked for a long time, it prevents execution of other actions, e.g. action triggered
    # by "Interrupt" button. This is why asyncio tasks should call "asyncio.sleep" periodically.
    # Having (1) and (2) in mind, we have to delegate blocking action into a separate thread and check its status
    # periodically in a non-blocking way.
    # This thread will exit automatically once job is cancelled.
    def await_first_row(self, execution_result: TableResult) -> None:
        try:
            execution_result.wait(self.first_row_polling_ms)
        except Py4JJavaError as err:
            # consume "job cancelled" error or rethrow any other
            if "org.apache.flink.runtime.client.JobCancellationException: Job was cancelled" not in str(err):
                print("Exception while waiting for rows.", err)
        except Exception as err:  # noqa: B902
            print("Exception while waiting for rows.", err)

    def get_job_status(self, client: JobClient) -> JobStatus:
        try:
            return client.get_job_status().result()
        except Py4JJavaError as err:
            # hack for "local" mode
            if "MiniCluster is not yet running or has already been shut down" in str(err):
                return JobStatus.FINISHED
            else:
                raise err

    async def __pull_results(self, execution_result: TableResult, display_row_kind: bool,
                             display_results: bool, pd_display_options: Optional[Dict[str, Any]] = None) -> None:
        """
        Pulls requests from given TableResult.
        :param execution_result: TableResult containing query results.
        :param display_row_kind: Indicates whether row kind should be displayed (insert/update_before/update_after)
        :param display_results: Indicates whether the query returns any results (e.g. query results or metadata).
        :param pd_display_options: Pandas display options.
        """
        if not pd_display_options:
            pd_display_options = {}

        await_first_row_thread = threading.Thread(target=self.await_first_row, args=(execution_result,))
        await_first_row_thread.start()

        # active polling
        while not self.interrupted:
            try:
                # Explicit await is needed to unblock the main thread to pick up other tasks.
                # In Jupyter's main execution pool there is only one worker thread.
                await asyncio.sleep(self.async_wait_s)
                # Wait until the first row is available or the job is finished/cancelled. Without the second condition,
                # the loop will never end if the job return empty result.
                if display_results:
                    client = execution_result.get_job_client()
                    # If client is None, then the result is returned immediately (e.g. metadata query).
                    if client is not None:
                        job_status = self.get_job_status(client)
                        if job_status is None:  # Job is not initialized yet
                            continue
                        if job_status in [JobStatus.CREATED, JobStatus.RUNNING]:
                            if await_first_row_thread.is_alive():
                                continue
                        else:  # job is finished/cancelled/failed
                            time.sleep(2)
                            # If job is done but the thread is still waiting for the first row, it means that the job
                            # has no results at all.
                            if await_first_row_thread.is_alive():
                                print("No results returned")
                                break
                    print("Pulling query results...")
                    await self.display_execution_result(execution_result, display_row_kind, pd_display_options)
                    return
                else:
                    # if finished then return early even if the user interrupts after this
                    # the actual invocation has already finished
                    await_first_row_thread.join()
                    print("Execution successful")
                    return
            except Py4JJavaError as err:
                # consume timeout error or rethrow any other
                if "java.util.concurrent.TimeoutException" not in str(err.java_exception):
                    raise err

        if self.interrupted:
            # await_first_row_thread will exit automatically once job is cancelled.
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
        print("Execution successful")

    async def display_execution_result(self, execution_result: TableResult, display_row_kind: bool,
                                       pd_display_options: Dict[str, Any]) -> pd.DataFrame:
        """
        Displays the execution result and returns a dataframe containing all the results.
        Display is done in a stream-like fashion displaying the results as they come.
        """

        columns = execution_result.get_table_schema().get_field_names()
        if display_row_kind:
            columns = ["row_kind"] + columns
        for key, value in pd_display_options.items():
            pd.set_option(key, value)
        df = pd.DataFrame(columns=columns)
        result_kind = execution_result.get_result_kind()

        if result_kind == ResultKind.SUCCESS_WITH_CONTENT:
            with execution_result.collect() as results:
                row_queue: queue.SimpleQueue[Optional[Row]] = queue.SimpleQueue()

                # Waiting for the next result in iterator is a blocking action, so "Interrupt" button does not trigger
                # any action until the next result comes. As a solution, there is a reader thread which puts results
                # into a queue, which in turn is read by a "display" thread in a non-blocking way.
                def row_reader() -> None:
                    try:
                        for result in results:
                            row_queue.put(result)
                        # None is a "poison pill"
                        row_queue.put(None)
                    except Py4JJavaError as e:
                        # consume "job cancelled" error or rethrow any other
                        if "org.apache.flink.runtime.client.JobCancellationException: Job was cancelled" not in str(e):
                            print("Exception while reading results.", e)
                    except Exception as e:  # noqa: B902
                        print("Exception while reading results.", e)

                row_reader_thread = threading.Thread(target=row_reader)
                row_reader_thread.start()

                print("Results will be pulled from the job. You can interrupt any time to show partial results.")
                print("Execution result will bind to `execution_result` variable.")
                rows_counter = IntText(value=0, description="Loaded rows: ")
                core_display(rows_counter)
                display_handle = None
                while not self.interrupted:
                    # Explicit await for the same reason as in `__internal_execute_sql`
                    await asyncio.sleep(self.async_wait_s)
                    try:
                        result = row_queue.get(timeout=1.0)
                        if result is None:  # None indicates that there will be no more results
                            break
                    except queue.Empty:
                        continue

                    res = list(result)
                    if display_row_kind:
                        res = [result.get_row_kind()] + res
                    a_series = pd.Series(res, index=df.columns)
                    df = pd.concat([df, pd.DataFrame.from_records([a_series])], ignore_index=True)
                    rows_counter.value += 1
                    if display_handle is None:
                        display_handle = display.display(df, display_id=True)
                    else:
                        display_handle.update(df)

        else:
            series = pd.Series(
                [pyflink_result_kind_to_string(result_kind)], index=df.columns
            )
            df = pd.concat([df, pd.DataFrame.from_records([series])], ignore_index=True)
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

        self.__extend_classpath(classpath_to_add)

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
        # https://stackoverflow.com/questions/48161387/python-how-to-print-the-stacktrace-of-an-exception-object-without-a-currently
        if fut.exception():
            print("Execution failed")
            print(fut.exception(), file=sys.stderr)
        else:
            print("Execution done")

    def __enrich_cell(self, cell: str) -> str:
        enriched_cell = CellContentFormatter(
            cell, {**os.environ, **self.shell.user_ns, **self._secrets}
        ).substitute_user_variables()
        joined_cell = inline_sql_in_cell(enriched_cell)
        return joined_cell

    def __flink_execute_sql_file(self, path: Union[str, os.PathLike[str]], display_row_kind: bool) -> None:
        if self.background_execution_in_progress:
            self.__retract_user_as_something_is_executing_in_background()
            return

        if os.path.exists(path):
            with open(path, "r") as f:
                statements = [self.__enrich_cell(s.rstrip(';')) for s in sqlparse.split(f.read())]
        else:
            return

        self.background_execution_in_progress = True
        self.deployment_bar.show_deployment_bar()
        try:
            self.__flink_execute_sql_file_internal(statements, display_row_kind)
        finally:
            self.background_execution_in_progress = False

    @line_magic
    @magic_arguments()
    @argument("-p", "--path", type=str, help="A path to a local file", required=True)
    @argument("--display-row-kind", help="Whether result row kind should be displayed", action="store_true")
    def flink_execute_file(self, line: str) -> None:
        args = parse_argstring(self.flink_execute_file, line)
        path = args.path
        if os.path.exists(path):
            self.__flink_execute_file(path, args.display_row_kind)
        else:
            print("File {} not found".format(path))

    def __flink_execute_file(self, path: Union[str, os.PathLike[str]], display_row_kind: bool) -> None:
        if self.background_execution_in_progress:
            self.__retract_user_as_something_is_executing_in_background()
            return

        with open(path, "r") as f:
            stmt = f.read()

        self.background_execution_in_progress = True
        self.deployment_bar.show_deployment_bar()
        try:
            self.___flink_execute_internal(stmt, display_row_kind=display_row_kind)
        finally:
            self.background_execution_in_progress = False

    def __extend_classpath(self, classpath_to_add: str) -> None:
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
        for jar in classpath_to_add.split(";"):
            print(f"Jar {jar} registered")

    def __load_plugins(self) -> None:
        if sys.version_info < (3, 10):
            from importlib_metadata import entry_points
        else:
            from importlib.metadata import entry_points
        for jar_provider in entry_points(group='catalog.jars.provider'):
            provider_f = jar_provider.load()
            classpath_to_add = provider_f()
            if classpath_to_add:
                self.__extend_classpath(classpath_to_add)

    @staticmethod
    def __retract_user_as_something_is_executing_in_background() -> None:
        print("Please wait for the previously submitted task to finish or cancel it.")

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

    @line_magic
    @magic_arguments()
    @argument("-t", "--type", type=str, default="tree", help="The widget type used to display schemas.")
    def flink_show_table_tree(self, line: str) -> Any:
        args = parse_argstring(self.flink_show_table_tree, line)
        widget_type = args.type

        schema = SchemaLoader(self.st_env).get_schema()
        if widget_type == "tree":
            return IPyTreeSchemaBuilder().build(schema)
        elif widget_type == "json":
            return JsonTreeSchemaBuilder().build(schema)
        raise ValueError(f"Unknown widget type '{widget_type}'.")


def load_ipython_extension(ipython: Any) -> None:
    ipython.register_magics(Integrations)
