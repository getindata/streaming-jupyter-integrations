import unittest

from streaming_jupyter_integrations.sql_utils import is_ddl, is_dml, is_query


# Some of the following examples are taken from Flink SQL documentation
# https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/overview/
class TestSqlUtils(unittest.TestCase):
    def test_simple_select(self):
        simple_select = "   SELECT   * FROM\n\n  myTable\n\t WHERE   someField\r = \r\nsomeOtherField \n\n "
        assert not is_ddl(simple_select)
        assert is_dml(simple_select)
        assert is_query(simple_select)

    def test_select_hints(self):
        select = """select * from
    kafka_table1 /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ t1
    join
    kafka_table2 /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ t2
    on t1.id = t2.id;
        """
        assert not is_ddl(select)
        assert is_dml(select)
        assert is_query(select)

    def test_with_query(self):
        with_query = "WITH orders_with_total AS (\n    " \
                     "SELECT order_id, price + tax AS total\n    " \
                     "FROM Orders\n)\nSELECT order_id, SUM(total)\n" \
                     "FROM orders_with_total\nGROUP BY order_id;"
        assert not is_ddl(with_query)
        assert is_dml(with_query)
        assert is_query(with_query)

    def test_select_distinct(self):
        select = "SELECT DISTINCT id FROM Orders"
        assert not is_ddl(select)
        assert is_dml(select)
        assert is_query(select)

    def test_select_values(self):
        select = "SELECT order_id, price FROM (VALUES (1, 2.0), (2, 3.1))  AS t (order_id, price)"
        assert not is_ddl(select)
        assert is_dml(select)
        assert is_query(select)

    def test_select_cumulate(self):
        select = """SELECT * FROM TABLE(
    CUMULATE(
      DATA => TABLE Bid,
      TIMECOL => DESCRIPTOR(bidtime),
      STEP => INTERVAL '2' MINUTES,
      SIZE => INTERVAL '10' MINUTES));"""
        assert not is_ddl(select)
        assert is_dml(select)
        assert is_query(select)

    def test_select_window_aggregation(self):
        select = """SELECT
  user,
  TUMBLE_START(order_time, INTERVAL '1' DAY) AS wStart,
  SUM(amount) FROM Orders
GROUP BY
  TUMBLE(order_time, INTERVAL '1' DAY),
  user"""
        assert not is_ddl(select)
        assert is_dml(select)
        assert is_query(select)

    def test_select_over_aggregation(self):
        select = """SELECT order_id, order_time, amount,
          SUM(amount) OVER w AS sum_amount,
          AVG(amount) OVER w AS avg_amount
        FROM Orders
        WINDOW w AS (
          PARTITION BY product
          ORDER BY order_time
          RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW)"""
        assert not is_ddl(select)
        assert is_dml(select)
        assert is_query(select)

    def test_create_table(self):
        create = "CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH ('connector'='raw');"
        assert is_ddl(create)
        assert not is_dml(create)
        assert not is_query(create)

        create_not_exist = "\n\nCREATE TABLE IF NOT EXISTS " \
                           "Catalog.Orders (`user` BIGINT, product STRING, " \
                           "amount INT) LIKE source_table (EXCLUDING PARTITIONS)"
        assert is_ddl(create_not_exist)
        assert not is_dml(create_not_exist)
        assert not is_query(create_not_exist)

    def test_create_catalog(self):
        create = "CREATE CATALOG Catalog WITH (key1  =  val1)"
        assert is_ddl(create)
        assert not is_dml(create)
        assert not is_query(create)

    def test_create_database(self):
        create = "CREATE DATABASE Catalog.Database WITH ('key1'  =  'val1')"
        assert is_ddl(create)
        assert not is_dml(create)
        assert not is_query(create)

    def test_create_view(self):
        create = "CREATE TEMPORARY VIEW IF NOT EXISTS MyView " \
                 "(`column1`, `index`, `column2`) AS (SELECT column1, index, column5 FROM someTable);"
        assert is_ddl(create)
        assert not is_dml(create)
        assert not is_query(create)

    def test_create_function(self):
        create = "CREATE FUNCTION MyFunction AS pyflink.table.tests.test_udf.add LANGUAGE PYTHON"
        assert is_ddl(create)
        assert not is_dml(create)
        assert not is_query(create)

    def test_drop_table(self):
        drop = "  DROP\n TABLE \n\rOrders"
        assert is_ddl(drop)
        assert not is_dml(drop)
        assert not is_query(drop)

    def test_drop_catalog(self):
        drop = "  DROP\n CATALOG IF EXISTS \n\rCatalog"
        assert is_ddl(drop)
        assert not is_dml(drop)
        assert not is_query(drop)

    def test_drop_database(self):
        drop = "\n\nDROP DATABASE Production\n\nCASCADE\n\n"
        assert is_ddl(drop)
        assert not is_dml(drop)
        assert not is_query(drop)

    def test_drop_view(self):
        drop = "DROP VIEW Catalog.Database.View"
        assert is_ddl(drop)
        assert not is_dml(drop)
        assert not is_query(drop)

    def test_drop_function(self):
        drop = "DROP FUNCTION Function"
        assert is_ddl(drop)
        assert not is_dml(drop)
        assert not is_query(drop)

    def test_alter_table(self):
        alter = "ALTER TABLE Orders RENAME TO Chaoses"
        assert is_ddl(alter)
        assert not is_dml(alter)
        assert not is_query(alter)

        alter_properties = "ALTER TABLE Orders SET (\n  'connector' = 'other_connector'\n);"
        assert is_ddl(alter_properties)
        assert not is_dml(alter_properties)
        assert not is_query(alter_properties)

    def test_alter_view(self):
        alter = "ALTER VIEW MyView RENAME TO YourView"
        assert is_ddl(alter)
        assert not is_dml(alter)
        assert not is_query(alter)

        alter_expression = "ALTER VIEW MyView AS (VALUES (1, 2, 3))"
        assert is_ddl(alter_expression)
        assert not is_dml(alter_expression)
        assert not is_query(alter_expression)

    def test_alter_database(self):
        alter = "ALTER DATABASE Production SET ('key1' = 'val2', 'key2' = 'val1'\r\t)"
        assert is_ddl(alter)
        assert not is_dml(alter)
        assert not is_query(alter)

    def test_alter_function(self):
        alter = "ALTER FUNCTION AddFunction AS pyflink.table.tests.test_udf.sub"
        assert is_ddl(alter)
        assert not is_dml(alter)
        assert not is_query(alter)

    def test_insert_from_select(self):
        insert = "INSERT INTO country_page_view " \
                 "PARTITION (date='2019-8-30', country='China')\n  " \
                 "SELECT user, cnt FROM page_view_source;"
        assert not is_ddl(insert)
        assert is_dml(insert)
        assert not is_query(insert)

        execute_insert = "EXECUTE  INSERT INTO country_page_view " \
                         "PARTITION (date='2019-8-30', country='China')\n  " \
                         "SELECT user, cnt FROM page_view_source;"
        assert not is_ddl(execute_insert)
        assert is_dml(execute_insert)
        assert not is_query(execute_insert)

        insert_overwrite_partition = "INSERT OVERWRITE country_page_view " \
                                     "PARTITION (date='2019-8-30')  \n  " \
                                     "SELECT user, cnt, country FROM page_view_source;"
        assert not is_ddl(insert_overwrite_partition)
        assert is_dml(insert_overwrite_partition)
        assert not is_query(insert_overwrite_partition)

        insert_append = "INSERT INTO country_page_view " \
                        "PARTITION (date='2019-8-30', country='China') (user)  " \
                        "SELECT user FROM page_view_source"
        assert not is_ddl(insert_append)
        assert is_dml(insert_append)
        assert not is_query(insert_append)

    def test_insert_values(self):
        insert = "INSERT INTO students\n  VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32)"
        assert not is_ddl(insert)
        assert is_dml(insert)
        assert not is_query(insert)

    def test_describe(self):
        describe = "DESCRIBE Orders"
        assert not is_ddl(describe)
        assert is_dml(describe)
        assert is_query(describe)

        desc = "DESC Database.Orders;"
        assert not is_ddl(desc)
        assert is_dml(desc)
        assert is_query(desc)

    def test_explain(self):
        explain = """
        EXPLAIN ESTIMATED_COST, CHANGELOG_MODE, JSON_EXECUTION_PLAN
        SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%'
        UNION ALL
        SELECT `count`, word FROM MyTable2
        """
        assert not is_ddl(explain)
        assert is_dml(explain)
        assert is_query(explain)

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
                self.assertFalse(is_query(use))

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
                self.assertTrue(is_dml(show))
                self.assertTrue(is_query(show))

    def test_load(self):
        load = "LOAD MODULE hive WITH ('hive-version' = '3.1.2')\n"
        self.assertTrue(is_ddl(load))
        self.assertFalse(is_dml(load))
        self.assertFalse(is_query(load))

    def test_unload(self):
        unload = "UNLOAD MODULE core"
        self.assertTrue(is_ddl(unload))
        self.assertFalse(is_dml(unload))
        self.assertFalse(is_query(unload))
