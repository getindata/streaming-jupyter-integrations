from typing import Any, Dict, List, Optional

from IPython.display import JSON
from ipytree import Node, Tree
from pyflink.common import Row
from pyflink.table import StreamTableEnvironment


class SchemaColumn:
    def __init__(self, name: str, type: str, null: Optional[str], key: Optional[str], extras: Optional[str],
                 watermark: Optional[str]):
        self.name = name
        self.type = type
        self.null = null
        self.key = key
        self.extras = extras
        self.watermark = watermark


class SchemaTable:
    def __init__(self, name: str, columns: List[SchemaColumn], is_view: bool):
        self.name = name
        self.columns = columns
        self.is_view = is_view


class SchemaFunction:
    def __init__(self, name: str):
        self.name = name


class SchemaDatabase:
    def __init__(self, name: str, tables: List[SchemaTable], functions: List[SchemaFunction]):
        self.name = name
        self.tables = tables
        self.functions = functions


class SchemaCatalog:
    def __init__(self, name: str, databases: List[SchemaDatabase]):
        self.name = name
        self.databases = databases


class SchemaRoot:
    def __init__(self, catalogs: List[SchemaCatalog]):
        self.catalogs = catalogs


class SchemaLoader:
    def __init__(self, st_env: StreamTableEnvironment):
        self.st_env = st_env

    def get_schema(self) -> SchemaRoot:
        # Save name of the current catalog and database. In order to build tables hierarchy, we need to change
        # current catalog and database. Once the table tree is built, the current catalog and database are set to
        # the initial ones.
        current_catalog_name = self.st_env.execute_sql("SHOW CURRENT CATALOG").collect().next()[0]
        current_database_name = self.st_env.execute_sql("SHOW CURRENT DATABASE").collect().next()[0]

        catalogs = self.st_env.execute_sql("SHOW CATALOGS").collect()
        catalogs_tree = [self._build_catalog(catalog) for catalog in catalogs]

        self.st_env.execute_sql(f"USE CATALOG `{current_catalog_name}`")
        self.st_env.execute_sql(f"USE `{current_database_name}`")

        return SchemaRoot(catalogs_tree)

    def _build_catalog(self, catalog: Row) -> SchemaCatalog:
        catalog_name = catalog[0]
        self.st_env.execute_sql(f"USE CATALOG `{catalog_name}`")
        databases = self.st_env.execute_sql("SHOW DATABASES").collect()
        tree_databases = [self._build_database(catalog_name, database) for database in databases]
        return SchemaCatalog(catalog_name, tree_databases)

    def _build_database(self, catalog_name: str, database: Row) -> SchemaDatabase:
        database_name = database[0]
        self.st_env.execute_sql(f"USE `{database_name}`")

        tables = [table[0] for table in self.st_env.execute_sql("SHOW TABLES").collect()]
        views = [view[0] for view in self.st_env.execute_sql("SHOW VIEWS").collect()]
        tree_tables = [self._build_table(catalog_name, database_name, table, table in views) for table in tables]

        functions = self.st_env.execute_sql("SHOW USER FUNCTIONS").collect()
        tree_functions = [SchemaFunction(function[0]) for function in functions]

        return SchemaDatabase(database_name, tree_tables, tree_functions)

    def _build_table(self, catalog_name: str, database_name: str, table_name: str, is_view: bool) -> SchemaTable:
        columns = self.st_env.execute_sql(f"DESCRIBE `{catalog_name}`.`{database_name}`.`{table_name}`").collect()
        tree_columns = [self._build_column(column) for column in columns]
        return SchemaTable(table_name, tree_columns, is_view)

    def _build_column(self, column: Row) -> SchemaColumn:
        return SchemaColumn(
            name=column[0],
            type=column[1],
            null=column[2],
            key=column[3],
            extras=column[4],
            watermark=column[5]
        )


class JsonTreeSchemaBuilder:
    def build(self, root: SchemaRoot) -> JSON:
        return JSON({catalog.name: self._build_catalog_value(catalog) for catalog in root.catalogs})

    def _build_catalog_value(self, catalog: SchemaCatalog) -> Dict[str, Any]:
        return {database.name: self._build_database_value(database) for database in catalog.databases}

    def _build_database_value(self, database: SchemaDatabase) -> Dict[str, Any]:
        return {table.name: self._build_table_value(table) for table in database.tables}

    def _build_table_value(self, table: SchemaTable) -> Dict[str, Any]:
        return {column.name: self._build_column_value(column) for column in table.columns}

    def _build_column_value(self, column: SchemaColumn) -> str:
        value = column.type
        if column.key:
            value += " PRIMARY KEY"
        if column.watermark:
            value += f" {column.watermark}"
        return value


class IPyTreeSchemaBuilder:
    def build(self, root: SchemaRoot) -> Tree:
        tree = Tree()
        for catalog in root.catalogs:
            tree.add_node(self._build_catalog_node(catalog))
        return tree

    def _build_catalog_node(self, catalog: SchemaCatalog) -> Node:
        tree_databases = [self._build_database_node(database) for database in catalog.databases]
        return Node(catalog.name, tree_databases, opened=False, icon="folder")

    def _build_database_node(self, database: SchemaDatabase) -> Node:
        tree_tables = [self._build_table_node(table) for table in database.tables]
        tree_tables.append(self._build_functions_node(database.functions))
        return Node(database.name, tree_tables, opened=False, icon="database")

    def _build_functions_node(self, functions: List[SchemaFunction]) -> Node:
        tree_functions = [Node(function.name, opened=False, icon="terminal") for function in functions]
        return Node("Functions", tree_functions, opened=False, icon="list")

    def _build_table_node(self, table: SchemaTable) -> Node:
        tree_columns = [self._build_column_node(column) for column in table.columns]
        return Node(table.name, tree_columns, opened=False, icon="eye" if table.is_view else "table")

    def _build_column_node(self, column: SchemaColumn) -> Node:
        column_display = f"{column.name}: {column.type}"
        column_display += " NULLABLE" if column.null else " NOT NULL"
        if column.extras:
            column_display += f" {column.extras}"
        if column.watermark:
            column_display += f" {column.watermark}"

        column_icon = "key" if column.key else "columns"
        return Node(column_display, opened=False, icon=column_icon)
