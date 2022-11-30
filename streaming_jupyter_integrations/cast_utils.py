import string
from random import choice

from pyflink.table import Table, TableEnvironment
from pyflink.table.types import DataType, LocalZonedTimestampType


def cast_timestamp_ltz_to_string(tenv: TableEnvironment, table: Table) -> 'Table':
    if not __contains_timestamp_ltz_fields(table):
        return table
    else:
        return __cast_timestamp_ltz_to_string(tenv, table)


def __contains_timestamp_ltz_fields(table: Table) -> bool:
    table_schema = table.get_schema()
    return any(__is_timestamp_ltz(t) for t in table_schema.get_field_data_types())


def __is_timestamp_ltz(field_type: DataType) -> bool:
    return isinstance(field_type, LocalZonedTimestampType)


def __cast_timestamp_ltz_to_string(tenv: TableEnvironment, table: Table) -> 'Table':
    table_schema = table.get_schema()
    cast_to_str_field_defs = []
    for field_name in table_schema.get_field_names():
        field_type = table_schema.get_field_data_type(field_name)
        if __is_timestamp_ltz(field_type):
            cast_to_str_field_defs.append(f"CAST(`{field_name}` AS STRING) AS `{field_name}`")
        else:
            cast_to_str_field_defs.append(f"`{field_name}`")
    raw_view_name = f"__raw_results_{__random_string()}"
    tenv.create_temporary_view(raw_view_name, table)
    cast_to_str_view_query = "SELECT \n" + ",\n".join(cast_to_str_field_defs) + f"\n FROM `{raw_view_name}`"
    return tenv.sql_query(cast_to_str_view_query)


def __random_string(length: int = 8) -> str:
    return "".join([choice(string.ascii_lowercase) for _ in range(length)])
