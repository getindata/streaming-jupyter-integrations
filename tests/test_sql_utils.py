import unittest

from streaming_jupyter_integrations.sql_utils import (inline_sql_in_cell,
                                                      is_ddl, is_dml, is_dql,
                                                      is_metadata_query)


# Some of the following examples are taken from Flink SQL documentation
# https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/overview/
class TestSqlUtils(unittest.TestCase):
    def test_simple_select(self):
        simple_select = "   SELECT   * FROM\n\n  myTable\n\t WHERE   someField\r = \r\nsomeOtherField \n\n "
        self.assertFalse(is_ddl(simple_select))
        self.assertFalse(is_dml(simple_select))
        self.assertTrue(is_dql(simple_select))
        self.assertFalse(is_metadata_query(simple_select))

    def test_select_hints(self):
        select = """select * from
    kafka_table1 /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ t1
    join
    kafka_table2 /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ t2
    on t1.id = t2.id;
        """
        self.assertFalse(is_ddl(select))
        self.assertFalse(is_dml(select))
        self.assertTrue(is_dql(select))
        self.assertFalse(is_metadata_query(select))

    def test_with_query(self):
        with_query = "WITH orders_with_total AS (\n    " \
                     "SELECT order_id, price + tax AS total\n    " \
                     "FROM Orders\n)\nSELECT order_id, SUM(total)\n" \
                     "FROM orders_with_total\nGROUP BY order_id;"
        self.assertFalse(is_ddl(with_query))
        self.assertFalse(is_dml(with_query))
        self.assertTrue(is_dql(with_query))
        self.assertFalse(is_metadata_query(with_query))

    def test_select_distinct(self):
        select = "SELECT DISTINCT id FROM Orders"
        self.assertFalse(is_ddl(select))
        self.assertFalse(is_dml(select))
        self.assertTrue(is_dql(select))
        self.assertFalse(is_metadata_query(select))

    def test_select_values(self):
        select = "SELECT order_id, price FROM (VALUES (1, 2.0), (2, 3.1))  AS t (order_id, price)"
        self.assertFalse(is_ddl(select))
        self.assertFalse(is_dml(select))
        self.assertTrue(is_dql(select))
        self.assertFalse(is_metadata_query(select))

    def test_select_cumulate(self):
        select = """SELECT * FROM TABLE(
    CUMULATE(
      DATA => TABLE Bid,
      TIMECOL => DESCRIPTOR(bidtime),
      STEP => INTERVAL '2' MINUTES,
      SIZE => INTERVAL '10' MINUTES));"""
        self.assertFalse(is_ddl(select))
        self.assertFalse(is_dml(select))
        self.assertTrue(is_dql(select))
        self.assertFalse(is_metadata_query(select))

    def test_select_window_aggregation(self):
        select = """SELECT
  user,
  TUMBLE_START(order_time, INTERVAL '1' DAY) AS wStart,
  SUM(amount) FROM Orders
GROUP BY
  TUMBLE(order_time, INTERVAL '1' DAY),
  user"""
        self.assertFalse(is_ddl(select))
        self.assertFalse(is_dml(select))
        self.assertTrue(is_dql(select))
        self.assertFalse(is_metadata_query(select))

    def test_select_over_aggregation(self):
        select = """SELECT order_id, order_time, amount,
          SUM(amount) OVER w AS sum_amount,
          AVG(amount) OVER w AS avg_amount
        FROM Orders
        WINDOW w AS (
          PARTITION BY product
          ORDER BY order_time
          RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW)"""
        self.assertFalse(is_ddl(select))
        self.assertFalse(is_dml(select))
        self.assertTrue(is_dql(select))
        self.assertFalse(is_metadata_query(select))

    def test_create_table(self):
        create = "CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH ('connector'='raw');"
        self.assertTrue(is_ddl(create))
        self.assertFalse(is_dml(create))
        self.assertFalse(is_dql(create))
        self.assertFalse(is_metadata_query(create))

        create_not_exist = "\n\nCREATE TABLE IF NOT EXISTS " \
                           "Catalog.Orders (`user` BIGINT, product STRING, " \
                           "amount INT) LIKE source_table (EXCLUDING PARTITIONS)"
        self.assertTrue(is_ddl(create_not_exist))
        self.assertFalse(is_dml(create_not_exist))
        self.assertFalse(is_dql(create_not_exist))
        self.assertFalse(is_metadata_query(create_not_exist))

    def test_create_catalog(self):
        create = "CREATE CATALOG Catalog WITH (key1  =  val1)"
        self.assertTrue(is_ddl(create))
        self.assertFalse(is_dml(create))
        self.assertFalse(is_dql(create))
        self.assertFalse(is_metadata_query(create))

    def test_create_database(self):
        create = "CREATE DATABASE Catalog.Database WITH ('key1'  =  'val1')"
        self.assertTrue(is_ddl(create))
        self.assertFalse(is_dml(create))
        self.assertFalse(is_dql(create))
        self.assertFalse(is_metadata_query(create))

    def test_create_view(self):
        create = "CREATE TEMPORARY VIEW IF NOT EXISTS MyView " \
                 "(`column1`, `index`, `column2`) AS (SELECT column1, index, column5 FROM someTable);"
        self.assertTrue(is_ddl(create))
        self.assertFalse(is_dml(create))
        self.assertFalse(is_dql(create))
        self.assertFalse(is_metadata_query(create))

    def test_create_function(self):
        create = "CREATE FUNCTION MyFunction AS pyflink.table.tests.test_udf.add LANGUAGE PYTHON"
        self.assertTrue(is_ddl(create))
        self.assertFalse(is_dml(create))
        self.assertFalse(is_dql(create))
        self.assertFalse(is_metadata_query(create))

    def test_drop_table(self):
        drop = "  DROP\n TABLE \n\rOrders"
        self.assertTrue(is_ddl(drop))
        self.assertFalse(is_dml(drop))
        self.assertFalse(is_dql(drop))
        self.assertFalse(is_metadata_query(drop))

    def test_drop_catalog(self):
        drop = "  DROP\n CATALOG IF EXISTS \n\rCatalog"
        self.assertTrue(is_ddl(drop))
        self.assertFalse(is_dml(drop))
        self.assertFalse(is_dql(drop))
        self.assertFalse(is_metadata_query(drop))

    def test_drop_database(self):
        drop = "\n\nDROP DATABASE Production\n\nCASCADE\n\n"
        self.assertTrue(is_ddl(drop))
        self.assertFalse(is_dml(drop))
        self.assertFalse(is_dql(drop))
        self.assertFalse(is_metadata_query(drop))

    def test_drop_view(self):
        drop = "DROP VIEW Catalog.Database.View"
        self.assertTrue(is_ddl(drop))
        self.assertFalse(is_dml(drop))
        self.assertFalse(is_dql(drop))
        self.assertFalse(is_metadata_query(drop))

    def test_drop_function(self):
        drop = "DROP FUNCTION Function"
        self.assertTrue(is_ddl(drop))
        self.assertFalse(is_dml(drop))
        self.assertFalse(is_dql(drop))
        self.assertFalse(is_metadata_query(drop))

    def test_alter_table(self):
        alter = "ALTER TABLE Orders RENAME TO Chaoses"
        self.assertTrue(is_ddl(alter))
        self.assertFalse(is_dml(alter))
        self.assertFalse(is_dql(alter))
        self.assertFalse(is_metadata_query(alter))

        alter_properties = "ALTER TABLE Orders SET (\n  'connector' = 'other_connector'\n);"
        self.assertTrue(is_ddl(alter_properties))
        self.assertFalse(is_dml(alter_properties))
        self.assertFalse(is_dql(alter_properties))
        self.assertFalse(is_metadata_query(alter_properties))

    def test_alter_view(self):
        alter = "ALTER VIEW MyView RENAME TO YourView"
        self.assertTrue(is_ddl(alter))
        self.assertFalse(is_dml(alter))
        self.assertFalse(is_dql(alter))
        self.assertFalse(is_metadata_query(alter))

        alter_expression = "ALTER VIEW MyView AS (VALUES (1, 2, 3))"
        self.assertTrue(is_ddl(alter_expression))
        self.assertFalse(is_dml(alter_expression))
        self.assertFalse(is_dql(alter_expression))
        self.assertFalse(is_metadata_query(alter_expression))

    def test_alter_database(self):
        alter = "ALTER DATABASE Production SET ('key1' = 'val2', 'key2' = 'val1'\r\t)"
        self.assertTrue(is_ddl(alter))
        self.assertFalse(is_dml(alter))
        self.assertFalse(is_dql(alter))
        self.assertFalse(is_metadata_query(alter))

    def test_alter_function(self):
        alter = "ALTER FUNCTION AddFunction AS pyflink.table.tests.test_udf.sub"
        self.assertTrue(is_ddl(alter))
        self.assertFalse(is_dml(alter))
        self.assertFalse(is_dql(alter))
        self.assertFalse(is_metadata_query(alter))

    def test_insert_from_select(self):
        insert = "INSERT INTO country_page_view " \
                 "PARTITION (date='2019-8-30', country='China')\n  " \
                 "SELECT user, cnt FROM page_view_source;"
        self.assertFalse(is_ddl(insert))
        self.assertTrue(is_dml(insert))
        self.assertFalse(is_dql(insert))
        self.assertFalse(is_metadata_query(insert))

        execute_insert = "EXECUTE  INSERT INTO country_page_view " \
                         "PARTITION (date='2019-8-30', country='China')\n  " \
                         "SELECT user, cnt FROM page_view_source;"
        self.assertFalse(is_ddl(execute_insert))
        self.assertTrue(is_dml(execute_insert))
        self.assertFalse(is_dql(execute_insert))
        self.assertFalse(is_metadata_query(execute_insert))

        execute_insert_newline = "EXECUTE \n\n\r\t\n INSERT INTO country_page_view\n\t " \
                                 "PARTITION (date='2019-8-30', country='China')\n  " \
                                 "SELECT user, cnt FROM page_view_source;"
        self.assertFalse(is_ddl(execute_insert_newline))
        self.assertTrue(is_dml(execute_insert_newline))
        self.assertFalse(is_dql(execute_insert_newline))
        self.assertFalse(is_metadata_query(execute_insert_newline))

        insert_overwrite_partition = "INSERT OVERWRITE country_page_view " \
                                     "PARTITION (date='2019-8-30')  \n  " \
                                     "SELECT user, cnt, country FROM page_view_source;"
        self.assertFalse(is_ddl(insert_overwrite_partition))
        self.assertTrue(is_dml(insert_overwrite_partition))
        self.assertFalse(is_dql(insert_overwrite_partition))
        self.assertFalse(is_metadata_query(insert_overwrite_partition))

        insert_append = "INSERT INTO country_page_view " \
                        "PARTITION (date='2019-8-30', country='China') (user)  " \
                        "SELECT user FROM page_view_source"
        self.assertFalse(is_ddl(insert_append))
        self.assertTrue(is_dml(insert_append))
        self.assertFalse(is_dql(insert_append))
        self.assertFalse(is_metadata_query(insert_append))

    def test_insert_values(self):
        insert = "INSERT INTO students\n  VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32)"
        self.assertFalse(is_ddl(insert))
        self.assertTrue(is_dml(insert))
        self.assertFalse(is_dql(insert))
        self.assertFalse(is_metadata_query(insert))

    def test_describe(self):
        describe = "DESCRIBE Orders"
        self.assertFalse(is_ddl(describe))
        self.assertFalse(is_dml(describe))
        self.assertTrue(is_dql(describe))
        self.assertTrue(is_metadata_query(describe))

        desc = "DESC Database.Orders;"
        self.assertFalse(is_ddl(desc))
        self.assertFalse(is_dml(desc))
        self.assertTrue(is_dql(desc))
        self.assertTrue(is_metadata_query(desc))

    def test_explain(self):
        explain = """
        EXPLAIN ESTIMATED_COST, CHANGELOG_MODE, JSON_EXECUTION_PLAN
        SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%'
        UNION ALL
        SELECT `count`, word FROM MyTable2
        """
        self.assertFalse(is_ddl(explain))
        self.assertFalse(is_dml(explain))
        self.assertTrue(is_dql(explain))
        self.assertTrue(is_metadata_query(explain))

    def test_use(self):
        uses = [
            "USE CATALOG CatalogName",
            "USE MODULES Module1, Module2, Module3",
            "USE SomeDatabase"
        ]
        for use in uses:
            with self.subTest(sql=use):
                self.assertTrue(is_ddl(use))
                self.assertFalse(is_dml(use))
                self.assertFalse(is_dql(use))
            self.assertFalse(is_metadata_query(use))

    def test_show_catalogs(self):
        shows = [
            "SHOW CATALOGS",
            "SHOW CURRENT CATALOG",
            "SHOW DATABASES",
            "SHOW CURRENT DATABASE",
            "show tables from db1",
            "show tables from db1 like '%n';",
            "show tables from db1 not like '%n';",
            "show tables in catalog1.db1 not like '%n';"
            "show tables",
            "SHOW CREATE TABLE",
            "show columns from catalog1.database1.orders",
            "show columns in database1.orders like '%r';",
            "show columns in catalog1.database1.orders not like '%_r';",
            "show views",
            "SHOW USER FUNCTIONS",
            "SHOW full modules",
            "show JARS"
        ]

        for show in shows:
            with self.subTest(sql=show):
                self.assertFalse(is_ddl(show))
                self.assertFalse(is_dml(show))
                self.assertTrue(is_dql(show))
                self.assertTrue(is_metadata_query(show))

    def test_load(self):
        load = "LOAD MODULE hive WITH ('hive-version' = '3.1.2')\n"
        self.assertTrue(is_ddl(load))
        self.assertFalse(is_dml(load))
        self.assertFalse(is_dql(load))
        self.assertFalse(is_metadata_query(load))

    def test_unload(self):
        unload = "UNLOAD MODULE core"
        self.assertTrue(is_ddl(unload))
        self.assertFalse(is_dml(unload))
        self.assertFalse(is_dql(unload))
        self.assertFalse(is_metadata_query(unload))

    def test_non_match(self):
        non_match = "  UNKNOWNABLEKEYWORD * FROM (SELECT * FROM SomeTable)"
        self.assertFalse(is_ddl(non_match))
        self.assertFalse(is_dml(non_match))
        self.assertFalse(is_dql(non_match))
        self.assertFalse(is_metadata_query(non_match))

    def test_multiline_cell(self):
        multiline_str = "CREATE TABLE multiline (\n"\
                        "id INT\n"\
                        ") WITH (\n"\
                        "'connector'='datagen')"
        self.assertEqual(inline_sql_in_cell(multiline_str),
                         "CREATE TABLE multiline ( id INT ) WITH ( 'connector'='datagen')")

    def test_comment(self):
        with_comments = "SELECT id\n"\
                        "--,name\n"\
                        "FROM table\n"\
                        "WHERE line = '--weird-line'"
        self.assertEqual(inline_sql_in_cell(with_comments), "SELECT id  FROM table WHERE line = '--weird-line'")
