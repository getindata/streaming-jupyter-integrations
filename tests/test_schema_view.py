import unittest

from IPython.display import JSON

from streaming_jupyter_integrations.schema_view import (JsonTreeSchemaBuilder,
                                                        SchemaCatalog,
                                                        SchemaColumn,
                                                        SchemaDatabase,
                                                        SchemaRoot,
                                                        SchemaTable)


class TestJsonTreeSchemaBuilder(unittest.TestCase):

    def test_conversion_to_json(self):
        # given
        default_catalog = SchemaCatalog(
            name="default_catalog",
            databases=[
                SchemaDatabase(
                    name="default_database",
                    tables=[
                        SchemaTable(
                            name="some_table",
                            is_view=False,
                            columns=[
                                SchemaColumn("column1", "bigint", None, None, None, None),
                                SchemaColumn("column2", "string", None, None, None, None),
                            ])
                    ],
                    functions=[]
                )
            ]
        )
        root = SchemaRoot(catalogs=[default_catalog])

        # when
        actual_schema_json = JsonTreeSchemaBuilder().build(root)

        # then
        expected_schema_json = JSON(
            {'default_catalog': {'default_database': {'some_table': {'column1': 'bigint', 'column2': 'string'}}}})
        self.assertEqual(actual_schema_json.data, expected_schema_json.data)
