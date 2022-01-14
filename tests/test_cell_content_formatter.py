import pytest

from streaming_jupyter_integrations.variable_substitution import (
    CellContentFormatter, NonExistentVariableException,
    VariableSyntaxException)


class TestCellContentFormatter:
    variables_in_kernel_context = {"some_variable": 1, "some_table_name": "table_name"}

    """Test Jupyter cell input text enrichement with variables defined in IPython kernel"""

    def test_variable_enrichements(self):
        cell_input_text = (
            "select * from {{ some_table_name }} v where v.id = { some_variable }"
        )
        enriched_cell = CellContentFormatter(
            input_string=cell_input_text,
            user_ns=TestCellContentFormatter.variables_in_kernel_context,
        ).substitute_user_variables()
        assert enriched_cell == "select * from table_name v where v.id = 1"

    """It should throw NonExistentVariableException in case of undefined variable"""

    def test_non_existent_variable_use(self):
        cell_input_text = "select * from {{ non_existent_variable }}"
        with pytest.raises(NonExistentVariableException):
            CellContentFormatter(
                input_string=cell_input_text,
                user_ns=TestCellContentFormatter.variables_in_kernel_context,
            ).substitute_user_variables()

    """It should throw VariableSyntaxException for variable names without whitespaces"""

    def test_wrong_variable_usage(self):
        cell_input_text = "select * from {{some_variable}}"
        with pytest.raises(VariableSyntaxException):
            CellContentFormatter(
                input_string=cell_input_text,
                user_ns=TestCellContentFormatter.variables_in_kernel_context,
            ).substitute_user_variables()
