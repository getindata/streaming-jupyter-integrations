import getpass
import os
import re
import string
import sys
from typing import Any, Dict, List, Optional, cast


def _match_forwards(full_string: str, match: Any, complimentary_regexp: str) -> str:
    start_pos = match.start()
    cut_string = full_string[start_pos:]
    full_match = re.match(complimentary_regexp, cut_string)
    if full_match:
        return full_match.group()
    return cut_string


def _match_backwards(full_string: str, match: Any, complimentary_regexp: str) -> str:
    end_pos = match.end()
    cut_string = full_string[:end_pos]
    matching_text = re.match(complimentary_regexp, cut_string[::-1])
    if matching_text:
        return matching_text.group()[::-1]
    return cut_string


# each ambiguous regexp has a corresponding (closing expression, directed matcher) tuple
# for a reverse search the reverse regexp is also reversed
ambiguous_regexps = {
    r"\{\{\S": (
        r"[^\}]*\}",
        _match_forwards,
    ),  # any two left braces followed by a non-whitespace character
    r"\S\}\}": (
        r"[^\{]*\{",
        _match_backwards,
    ),  # any non-whitespace character followed by two braces
    r"\{\s+\{": (r"[^\}]*\}", _match_forwards),  # catch some attempted nesting cases
    r"\}\s+\}": (r"[^\{]*\{", _match_backwards),  # catch some attempted nesting cases
}


# We need to skip curly brackets if there's no variable within, it can be regexp etc
class SafeDict(Dict[Any, Any]):
    def __missing__(self, key: str) -> str:
        if any(c.isalpha() for c in key):
            print(f"Variable '{key}' not found. The substitution will be skipped.", file=sys.stderr)
        return '{' + key + '}'


class CellContentFormatter(string.Formatter):
    def __init__(self, input_string: str, user_ns: Dict[Any, Any]):
        self.input_string = input_string
        self.user_ns = user_ns
        self.hidden_vars: Dict[str, str] = {}

    def substitute_user_variables(self) -> str:
        ambiguous_syntax = self._get_ambiguous_syntax(self.input_string)
        if ambiguous_syntax:
            raise VariableSyntaxException(
                ambiguous_syntax,
                message="Found the string {}, which is ambiguous or invalid.".format(
                    ambiguous_syntax
                ),
            )
        escaped_string = self._prepare_escaped_variables(self.input_string)
        escaped_string = self._get_hidden_variables(escaped_string)
        return self._substitute_variables(escaped_string)

    @classmethod
    def _get_ambiguous_syntax(cls, input_text: str) -> Optional[str]:
        for regexp in ambiguous_regexps.keys():
            match = re.search(regexp, input_text)
            if match:
                return cls._match_help_expression(regexp, input_text, match)
        return None

    @classmethod
    def _match_help_expression(cls, regexp: str, full_string: str, match: Any) -> str:
        complimentary_regexp = ambiguous_regexps[regexp][0]
        match_function = ambiguous_regexps[regexp][1]
        return match_function(full_string, match, complimentary_regexp)

    @staticmethod
    def _prepare_escaped_variables(input_text: str) -> str:
        return (
            input_text.replace("{{", "{")
            .replace("}}", "}")
            .replace("{ ", "{")
            .replace(" }", "}")
        )

    def _get_hidden_variables(self, escaped_string: str) -> str:
        hidden_var_pattern = r"(\$\{[_a-zA-Z][_a-zA-Z0-9]*\})"
        hidden_list = cast(List[str], re.findall(hidden_var_pattern, escaped_string))
        for hidden in hidden_list:
            var_name = hidden[2:-1].strip()
            self.hidden_vars[var_name] = os.environ.get(var_name, "")
            while not self.hidden_vars.get(var_name):
                print(f"Please input '{var_name}':\n")
                var_val = getpass.getpass()
                self.hidden_vars[var_name] = var_val
        return escaped_string.replace("${", "{")

    def _substitute_variables(self, escaped_string: str) -> str:
        try:
            merged_vars = {**self.user_ns, **self.hidden_vars}  # hidden_vars takes precedence
            result = escaped_string.format_map(SafeDict(**merged_vars))
            self.hidden_vars.clear()  # ensure it gets wiped out after using
            return result
        except KeyError as error:
            raise NonExistentVariableException(
                error.args[0],
                message="The variable {} is not defined but is referenced in the cell.".format(
                    error
                ),
                sql=self.input_string,
            )


class NonExistentVariableException(Exception):
    def __init__(self, variable_name: str, message: str = "", sql: Optional[str] = None):
        super(NonExistentVariableException, self).__init__(message)
        self.sql = sql
        self.variable_name = variable_name


class VariableSyntaxException(Exception):
    def __init__(self, bad_text: str, message: str = "", sql: Optional[str] = None):
        super(VariableSyntaxException, self).__init__(message)
        self.bad_text = bad_text
        self.sql = sql
