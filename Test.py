class InventoryTestSuite:
    def setup(self):
        # Create the necessary tables and insert test data
        spark.sql("DROP TABLE IF EXISTS inventory_change")
        spark.sql("DROP TABLE IF EXISTS Store")
        spark.sql("DROP TABLE IF EXISTS inventory_change_type")
        spark.sql("DROP TABLE IF EXISTS latestinventorysnapshot")

        spark.sql("""
        CREATE TABLE inventory_change (
            store_id INT,
            item_id INT,
            date_time TIMESTAMP,
            quantity INT
        ) USING DELTA
        """)

        spark.sql("""
        INSERT INTO inventory_change VALUES
        (1, 101, '2023-10-01 10:00:00', 10),
        (1, 102, '2023-10-01 11:00:00', 5),
        (2, 101, '2023-10-01 12:00:00', -3)
        """)

        spark.sql("""
        CREATE TABLE Store (
            store_id INT,
            name STRING
        ) USING DELTA
        """)

        spark.sql("""
        INSERT INTO Store VALUES
        (1, 'physical'),
        (2, 'online')
        """)

        spark.sql("""
        CREATE TABLE inventory_change_type (
            change_type_id INT,
            change_type STRING
        ) USING DELTA
        """)

        spark.sql("""
        INSERT INTO inventory_change_type VALUES
        (1, 'sale'),
        (2, 'bopis')
        """)

        spark.sql("""
        CREATE TABLE latestinventorysnapshot (
            store_id INT,
            item_id INT,
            date_time TIMESTAMP,
            quantity INT
        ) USING DELTA
        """)

        spark.sql("""
        INSERT INTO latestinventorysnapshot VALUES
        (1, 101, '2023-09-30 10:00:00', 100),
        (1, 102, '2023-09-30 11:00:00', 50),
        (2, 101, '2023-09-30 12:00:00', 30)
        """)

    def test_inventory_logic(self):
        from pyspark.sql.functions import expr, col, first, sum, max, coalesce, lit

        # Read the tables
        inventory_change_df = spark.read.table('inventory_change')
        store_df = spark.read.table('Store')
        inventory_change_type_df = spark.read.table('inventory_change_type')
        latest_inventory_snapshot_df = spark.read.table('latestinventorysnapshot')

        # Perform the joins and transformations as defined in the DLT pipeline
        inventory_change_df = (
            inventory_change_df.alias('x')
            .join(store_df.alias('y'), on='store_id')
            .join(inventory_change_type_df.alias('z'), on=col('change_type_id') == col('z.change_type_id'))
            .filter(expr("NOT(y.name='online' AND z.change_type='bopis')"))
            .select('store_id', 'item_id', 'date_time', 'quantity', 'change_type_id')
        )

        inventory_current_df = (
            latest_inventory_snapshot_df.alias('a')
            .join(
                inventory_change_df.alias('b'),
                on=expr('''
                    a.store_id=b.store_id AND 
                    a.item_id=b.item_id AND 
                    a.date_time<=b.date_time
                '''),
                how='leftouter'
            )
            .groupBy('a.store_id', 'a.item_id')
            .agg(
                first('a.quantity').alias('snapshot_quantity'),
                sum('b.quantity').alias('change_quantity'),
                first('a.date_time').alias('snapshot_datetime'),
                max('b.date_time').alias('change_datetime')
            )
            .withColumn('change_quantity', coalesce('change_quantity', lit(0)))
            .withColumn('current_quantity', expr('snapshot_quantity + change_quantity'))
            .withColumn('date_time', expr('GREATEST(snapshot_datetime, change_datetime)'))
            .drop('snapshot_datetime', 'change_datetime')
            .orderBy('current_quantity')
        )

        # Display the final DataFrame
        display(inventory_current_df)
        print("Test inventory logic successfully done")

    def teardown(self):
        # Drop the tables
        spark.sql("DROP TABLE IF EXISTS inventory_change")
        spark.sql("DROP TABLE IF EXISTS Store")
        spark.sql("DROP TABLE IF EXISTS inventory_change_type")
        spark.sql("DROP TABLE IF EXISTS latestinventorysnapshot")
        print("Teardown successfully done")

# Run the test suite
test_suite = InventoryTestSuite()
test_suite.setup()
test_suite.test_inventory_logic()
test_suite.teardown()
