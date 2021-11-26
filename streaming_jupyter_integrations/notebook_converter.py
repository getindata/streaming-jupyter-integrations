import argparse
import shlex
import sys
from dataclasses import dataclass, field
from typing import Optional

import click
import nbformat
from jinja2 import Environment
from streamingcli.project.template_loader import TemplateLoader
from streamingcli.jupyter.config_file_loader import ConfigFileLoader
import autopep8


@dataclass
class ConvertedNotebook:
    content: str
    jars: list = field(default_factory=lambda: [])


@dataclass
class NotebookEntry:
    type: str


@dataclass
class Sql(NotebookEntry):
    value: str = ""
    type: str = "SQL"


@dataclass
class RegisterUdf(NotebookEntry):
    function_name: str = ""
    object_name: str = ""
    type: str = "REGISTER_UDF"


@dataclass
class RegisterJavaUdf(RegisterUdf):
    type: str = "REGISTER_JAVA_UDF"


@dataclass
class Code(NotebookEntry):
    value: str = ""
    type: str = "CODE"


@dataclass
class RegisterJar(NotebookEntry):
    url: str = ""
    type: str = "REGISTER_JAR"


@dataclass
class RegisterLocalJar(NotebookEntry):
    local_path: str = ""
    type: str = "REGISTER_LOCAL_JAR"


class NotebookConverter:
    _register_udf_parser = argparse.ArgumentParser()
    _register_udf_parser.add_argument("--function_name")
    _register_udf_parser.add_argument("--object_name")
    _register_udf_parser.add_argument("--language")
    _register_udf_parser.add_argument("--remote_path")
    _register_udf_parser.add_argument("--local_path")

    _register_loadconfig_parser = argparse.ArgumentParser()
    _register_loadconfig_parser.add_argument("--path")

    @staticmethod
    def convert_notebook(notebook_path: str) -> ConvertedNotebook:
        try:
            notebook = NotebookConverter.load_notebook(notebook_path)
            code_cells = filter(lambda _: _.cell_type == 'code', notebook.cells)
            script_entries = []
            for cell in code_cells:
                entry = NotebookConverter.get_notebook_entry(cell)
                if entry is not None:
                    script_entries.append(entry)
            return NotebookConverter.render_flink_app(script_entries)
        except IOError:
            raise click.ClickException(f"Could not open file: {notebook_path}")
        except:
            raise click.ClickException(
                f"Unexpected exception: {sys.exc_info()}")

    @staticmethod
    def load_notebook(notebook_path: str) -> nbformat.NotebookNode:
        with open(notebook_path, 'r+') as notebook_file:
            return nbformat.reads(notebook_file.read(), as_version=4)

    @staticmethod
    def get_notebook_entry(cell) -> Optional[NotebookEntry]:
        if cell.source.startswith('%'):
            return NotebookConverter.handle_magic_cell(cell)
        elif not cell.source.startswith('##'):
            return Code(value=cell.source)

    @staticmethod
    def handle_magic_cell(cell) -> Optional[NotebookEntry]:
        source = cell.source
        if source.startswith('%%flink_execute_sql'):
            return Sql(value='\n'.join(source.split('\n')[1:]))
        if source.startswith('%flink_register_function'):
            args = NotebookConverter._register_udf_parser.parse_args(shlex.split(source)[1:])
            return RegisterJavaUdf(function_name=args.function_name,
                                   object_name=args.object_name,
                                   ) if args.language == 'java' else \
                RegisterUdf(function_name=args.function_name,
                            object_name=args.object_name)
        if cell.source.startswith(('%load_config_file', '##')):
            args = NotebookConverter._register_loadconfig_parser.parse_args(shlex.split(source)[1:])
            loaded_variables = ConfigFileLoader.load_config_file(path=args.path)
            variable_strings = []
            for v in loaded_variables:
                if isinstance(loaded_variables[v], str):
                    variable_strings.append(f"{v}=\"{loaded_variables[v]}\"")
                else:
                    variable_strings.append(f"{v}={loaded_variables[v]}")
            all_variable_strings = "\n".join(variable_strings)
            return Code(value=f"{all_variable_strings}")
        if source.startswith('%flink_register_jar'):
            args = NotebookConverter._register_udf_parser.parse_args(shlex.split(source)[1:])
            return RegisterJar(url=args.remote_path) if args.remote_path is not None else \
                RegisterLocalJar(local_path=args.local_path)
        return None

    @staticmethod
    def render_flink_app(notebook_entries: list) -> ConvertedNotebook:
        flink_app_template = TemplateLoader.load_project_template("flink_app.py.template")
        flink_app_script = Environment().from_string(flink_app_template).render(
            notebook_entries=notebook_entries
        )
        jars = map(lambda entry: entry.url, filter(lambda entry: isinstance(entry, RegisterJar), notebook_entries))
        return ConvertedNotebook(content=autopep8.fix_code(flink_app_script), jars=list(jars))
