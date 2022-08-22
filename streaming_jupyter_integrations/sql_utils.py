from typing import Iterable

import sqlparse

DDL_KEYWORDS = {
    'CREATE',
    'DROP',
    'ALTER',
    'USE',
    'LOAD',
    'UNLOAD'
}

QUERY_KEYWORDS = {
    'VALUES',
    'WITH',  # WITH ... query
    'SELECT'
}

DQL_KEYWORDS = {
    *QUERY_KEYWORDS,
    'DESCRIBE',
    'DESC',
    'EXPLAIN',
    'SHOW'
}

DML_KEYWORDS = {
    "INSERT",
    "EXECUTE"
}


def inline_sql_in_cell(cell_contents: str) -> str:
    """
    Converts cell contents to a single line SQL statement.
    """
    return cell_contents.replace("\n", " ")


def is_ddl(sql: str) -> bool:
    return __first_token_is_keyword(sql, DDL_KEYWORDS)


def is_dml(sql: str) -> bool:
    return __first_token_is_keyword(sql, DML_KEYWORDS)


def is_dql(sql: str) -> bool:
    return __first_token_is_keyword(sql, DQL_KEYWORDS)


def __first_token_is_keyword(sql: str, keywords: Iterable[str]) -> bool:
    if not sql or not sql.strip():
        return False

    parsed = sqlparse.parse(sql.upper())[0]
    first_token = parsed.token_first()
    return first_token.value in keywords
