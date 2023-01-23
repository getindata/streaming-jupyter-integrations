import contextlib
import io
from unittest.mock import patch

import pytest

from streaming_jupyter_integrations.variable_substitution import (
    CellContentFormatter, VariableSyntaxException)


class TestCellContentFormatter:
    variables_in_kernel_context = {"some_variable": 1, "some_table_name": "table_name"}

    def test_variable_enrichments(self):
        """Test Jupyter cell input text enrichment with variables defined in IPython kernel"""
        cell_input_text = (
            "select * from {{ some_table_name }} v where v.id = { some_variable }"
        )
        enriched_cell = CellContentFormatter(
            input_string=cell_input_text,
            user_ns=TestCellContentFormatter.variables_in_kernel_context,
        ).substitute_user_variables()
        assert enriched_cell == "select * from table_name v where v.id = 1"

    def test_hidden_variable_enrichments(self):
        """
        Test Jupyter cell input text enrichment with variables defined in
        IPython kernel and got by getpass method.

        This test uses the same variable name (`some_variable`) to check and
        ensure that: (1) when `${}` is present, it gets precedence over already
        defined variables; and (2) variables read using getpass do not mess up
        IPython kernel namespace.
        """

        cell_input_text_prefix = "create table {{ some_table_name }} (\n  id INT\n) WITH " \
                                 "(\n  'connector' = 'database',\n  'password' = "
        cell_input_text = cell_input_text_prefix + "'{ some_variable }'\n)"  # user namespace variable
        cell_input_text_hidden = cell_input_text_prefix + "'${ some_variable }'\n)"  # getpass variable

        with patch("getpass.getpass", lambda: "s3cr3tPassw0rd"):
            # first, we format `cell_input_text_hidden` so some_variable will be replaced by getpass input
            cell_content_formatter = CellContentFormatter(
                input_string=cell_input_text_hidden,
                user_ns=TestCellContentFormatter.variables_in_kernel_context,
            )
            enriched_cell = cell_content_formatter.substitute_user_variables()
            assert enriched_cell == "create table table_name (\n  id INT\n) WITH " \
                                    "(\n  'connector' = 'database',\n  'password' = 's3cr3tPassw0rd'\n)"
            assert not cell_content_formatter.hidden_vars  # is empty

            # then, we format `cell_input_text_hidden` so some_variable will be replaced by
            # variable from the dictionary passed as `user_ns`
            enriched_cell = CellContentFormatter(
                input_string=cell_input_text,
                user_ns=TestCellContentFormatter.variables_in_kernel_context,
            ).substitute_user_variables()
            assert enriched_cell == "create table table_name (\n  id INT\n) WITH " \
                                    "(\n  'connector' = 'database',\n  'password' = '1'\n)"

    def test_hidden_variable_enrichment_using_env_var(self):
        """
        Test Jupyter cell input text enrichment with variables defined in
        IPython kernel, environment and got by getpass method.
        """

        cell_input_text = """create table {{ some_table_name }} (
  id INT
) WITH (
  'connector' = 'database',
  'password' = '${ some_variable }',
  'username' = '${username}'
)"""

        with patch("getpass.getpass", lambda: "s3cr3tPassw0rd"), patch.dict("os.environ", {"username": "u532n4m3"}):
            # first, we format `cell_input_text_hidden` so some_variable will be replaced by getpass input
            cell_content_formatter = CellContentFormatter(
                input_string=cell_input_text,
                user_ns=TestCellContentFormatter.variables_in_kernel_context,
            )
            enriched_cell = cell_content_formatter.substitute_user_variables()
            assert enriched_cell == """create table table_name (
  id INT
) WITH (
  'connector' = 'database',
  'password' = 's3cr3tPassw0rd',
  'username' = 'u532n4m3'
)"""
            assert not cell_content_formatter.hidden_vars  # is empty

    def test_non_existent_variable_use(self):
        """It should print error in case of undefined variable"""
        cell_input_text = "select * from {{ non_existent_variable }}"
        f = io.StringIO()
        with contextlib.redirect_stderr(f):
            CellContentFormatter(
                input_string=cell_input_text,
                user_ns=TestCellContentFormatter.variables_in_kernel_context,
            ).substitute_user_variables()

        assert "Variable 'non_existent_variable' not found. The substitution will be skipped." == f.getvalue().strip()

    def test_wrong_variable_usage(self):
        """It should throw VariableSyntaxException for variable names without whitespaces"""
        cell_input_text = "select * from {{some_variable}}"
        with pytest.raises(VariableSyntaxException):
            CellContentFormatter(
                input_string=cell_input_text,
                user_ns=TestCellContentFormatter.variables_in_kernel_context,
            ).substitute_user_variables()

    def test_skip_substitution_if_not_variable(self):
        """It should skip brackets if something is not a variable"""
        cell_input_text = (
            "select * from {{ some_table_name }} v where v.id = 'abc{1,3}'"
        )
        enriched_cell = CellContentFormatter(
            input_string=cell_input_text,
            user_ns=TestCellContentFormatter.variables_in_kernel_context,
        ).substitute_user_variables()
        assert enriched_cell == "select * from table_name v where v.id = 'abc{1,3}'"

    def test_skip_numerics_in_curly_brackets(self):
        """It should skip brackets if something in brackets is numerical"""
        cell_input_text = (
            "select * from {{ some_table_name }} v where v.id = 'abc{1}'"
        )
        enriched_cell = CellContentFormatter(
            input_string=cell_input_text,
            user_ns=TestCellContentFormatter.variables_in_kernel_context,
        ).substitute_user_variables()
        assert enriched_cell == "select * from table_name v where v.id = 'abc{1}'"
