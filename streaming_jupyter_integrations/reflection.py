import ast
import inspect


def get_decorators_for_method(class_):
    """
    Gets all methods inside a class.
    Maps all methods inside a class to their corresponding decorators.
    @param class_: A class to perform the method / decorators aggregation onto.
    @return: A dictionary of {method -> [decorator]}
    """
    decorators_for_method = {}

    def visit_ast_node(node):
        decorators_for_method[node.name] = [ast.dump(e) for e in node.decorator_list]

    visitor_pattern = ast.NodeVisitor()
    visitor_pattern.visit_FunctionDef = visit_ast_node
    visitor_pattern.visit(compile(inspect.getsource(class_), '?', 'exec', ast.PyCF_ONLY_AST))
    return decorators_for_method


def get_method_names_for(class_, decorator_name: str) -> [str]:
    """
    Returns all method names inside a class that are decorated with decorator_name.
    @param class_: A class in which all methods will be filtered to the ones containing the specific decorator.
    @param decorator_name: A decorator name to find in the class' methods.
    @return: A list of names of the methods of the class
    """
    decorators_for_method = get_decorators_for_method(class_)
    filtered_decorators_for_method = {method_name: method_decorators
                                      for method_name, method_decorators in decorators_for_method.items()
                                      if any(decorator_name in method_decorator
                                             for method_decorator in method_decorators)}
    return list(filtered_decorators_for_method.keys())
