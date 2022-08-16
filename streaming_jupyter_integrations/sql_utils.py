from typing import Dict, Iterable, List

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

DML_PREFIXES: Dict[str, List[str]] = {
    'INSERT': [],
    'EXECUTE': ['INSERT'],
    **{dql: [] for dql in DQL_KEYWORDS}
}


def inline_sql_in_cell(cell_contents: str) -> str:
    """
    Converts cell contents to a single line SQL statement.
    """
    return cell_contents.replace("\n", " ")


def is_ddl(sql: str) -> bool:
    return __first_token_is_keyword(sql, DDL_KEYWORDS)


def is_dml(sql: str) -> bool:
    if not sql or not sql.strip():
        return False

    parsed = sqlparse.parse(sql.upper())[0]
    (first_token_idx, first_token) = parsed.token_next(-1)
    dml_follow_up = DML_PREFIXES.get(first_token.value)
    if dml_follow_up is None:
        return False
    if len(dml_follow_up) == 0:
        return True
    (_, next_token) = parsed.token_next(first_token_idx)
    return next_token.value in dml_follow_up


def is_query(sql: str) -> bool:
    return __first_token_is_keyword(sql, DQL_KEYWORDS)


def __first_token_is_keyword(sql: str, keywords: Iterable[str]) -> bool:
    if not sql or not sql.strip():
        return False

    parsed = sqlparse.parse(sql.upper())[0]
    first_token = parsed.token_first()
    return first_token.value in keywords
