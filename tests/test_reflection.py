import collections

from IPython.core.magic import cell_magic, line_magic

from streaming_jupyter_integrations.reflection import (
    get_decorators_for_method, get_method_names_for)


class TestReflection:
    """Test reflection of method names with cell magic over a custom class"""

    def test_get_method_names_for_cell_magic(self):
        method_list = get_method_names_for(ReflectionTestClass, "cell_magic")

        assert collections.Counter(
            ["static_method_with_cell_magic", "method_with_cell_magic"]
        ) == collections.Counter(method_list)

    """Test reflection of method names with line magic over a custom class"""

    def test_get_method_names_for_line_magic(self):
        method_list = get_method_names_for(ReflectionTestClass, "line_magic")

        assert collections.Counter(["method_with_other_magic"]) == collections.Counter(
            method_list
        )

    """Test reflection of methods with decorators over a custom class"""

    def test_get_decorators_for_method(self):
        decorators_for_methods = get_decorators_for_method(ReflectionTestClass)

        # all methods should be listed
        assert (
            collections.Counter(
                [
                    "method_without_decorator",
                    "static_method_with_cell_magic",
                    "method_with_cell_magic",
                    "method_with_other_magic",
                ]
            )
            == collections.Counter(list(decorators_for_methods.keys()))
        )
        # checking all the decorators
        assert collections.Counter([]) == collections.Counter(
            decorators_for_methods["method_without_decorator"]
        )
        assert collections.Counter(
            ["Name(id='cell_magic', ctx=Load())", "Name(id='staticmethod', ctx=Load())"]
        ) == collections.Counter(
            decorators_for_methods["static_method_with_cell_magic"]
        )
        assert collections.Counter(
            ["Name(id='cell_magic', ctx=Load())"]
        ) == collections.Counter(decorators_for_methods["method_with_cell_magic"])
        assert collections.Counter(
            ["Name(id='line_magic', ctx=Load())"]
        ) == collections.Counter(decorators_for_methods["method_with_other_magic"])


class ReflectionTestClass:
    def method_without_decorator(self):
        pass

    @staticmethod
    @cell_magic
    def static_method_with_cell_magic():
        pass

    @cell_magic
    def method_with_cell_magic(self):
        pass

    @line_magic
    def method_with_other_magic(self):
        pass
