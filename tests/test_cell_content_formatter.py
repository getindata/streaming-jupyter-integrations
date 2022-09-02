from unittest.mock import patch

import pytest

from streaming_jupyter_integrations.variable_substitution import (
    CellContentFormatter, NonExistentVariableException,
    VariableSyntaxException)


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
        Test Jupyter cell input text enrichment with both variables defined in
        IPython kernel and get by getpass method
        """

        cell_input_text_prefix = "create table {{ some_table_name }} (\n  id INT\n) WITH " \
                                 "(\n  'connector' = 'database',\n  'password' = "
        cell_input_text = cell_input_text_prefix + "'{ some_variable }'\n)"
        cell_input_text_hidden = cell_input_text_prefix + "'${ some_variable }'\n)"

        with patch("getpass.getpass", lambda: "s3cr3tPassw0rd"):
            cell_content_formatter = CellContentFormatter(
                input_string=cell_input_text_hidden,
                user_ns=TestCellContentFormatter.variables_in_kernel_context,
            )
            enriched_cell = cell_content_formatter.substitute_user_variables()
            assert enriched_cell == "create table table_name (\n  id INT\n) WITH " \
                                    "(\n  'connector' = 'database',\n  'password' = 's3cr3tPassw0rd'\n)"
            assert not cell_content_formatter.hidden_vars  # is empty

        enriched_cell = CellContentFormatter(
            input_string=cell_input_text,
            user_ns=TestCellContentFormatter.variables_in_kernel_context,
        ).substitute_user_variables()
        assert enriched_cell == "create table table_name (\n  id INT\n) WITH " \
                                "(\n  'connector' = 'database',\n  'password' = '1'\n)"

    def test_non_existent_variable_use(self):
        """It should throw NonExistentVariableException in case of undefined variable"""
        cell_input_text = "select * from {{ non_existent_variable }}"
        with pytest.raises(NonExistentVariableException):
            CellContentFormatter(
                input_string=cell_input_text,
                user_ns=TestCellContentFormatter.variables_in_kernel_context,
            ).substitute_user_variables()

    def test_wrong_variable_usage(self):
        """It should throw VariableSyntaxException for variable names without whitespaces"""
        cell_input_text = "select * from {{some_variable}}"
        with pytest.raises(VariableSyntaxException):
            CellContentFormatter(
                input_string=cell_input_text,
                user_ns=TestCellContentFormatter.variables_in_kernel_context,
            ).substitute_user_variables()
