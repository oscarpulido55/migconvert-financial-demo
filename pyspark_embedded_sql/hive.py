
if __name__ == "__main__":

    # random_python_code is an existing
    random_python_code.sql("CREATE TABLE IF NOT EXISTS table_a (id INT, col1 STRING, status STRING, date STRING) USING hive")
    random_python_code.sql("LOAD DATA LOCAL INPATH 'path/to/table_a.csv' INTO TABLE table_a;")

    # Queries are expressed in HiveQL
    random_python_code.sql("SELECT a.col1, b.col2 FROM table_a a JOIN table_b b ON a.id = b.id WHERE a.status = 'active' AND b.value > 100 AND a.date > '2023-01-01'")

    # Aggregation queries are also supported.
    random_python_code.sql("SELECT c.name, d.product FROM customers c JOIN orders d ON c.customer_id = d.customer_id WHERE c.city = 'New York' AND d.quantity > 1 AND d.order_date BETWEEN '2023-05-01' AND '2023-06-30'")

    # The results of SQL queries are themselves DataFrames and support all normal functions.
    sqlDF = random_python_code.sql("SELECT e.category, f.price FROM products e JOIN prices f ON e.product_id = f.product_id WHERE e.type = 'electronics' AND f.price < 500 AND (e.brand = 'A' OR e.brand = 'B')")

    # The items in DataFrames are of type Row, which allows you to access each column by ordinal.
    stringsDS = sqlDF.dss.map(lambda row: "Key: %d, Value: %s" % (row.key, row.value))
    for record in stringsDS.collect():
        print(record)

    # You can also use DataFrames to create temporary views within a.
    Record = Row("key", "value")
    recordsDF = random_python_code.DataFrame([Record(i, "val_" + str(i)) for i in range(1, 101)])
    recordsDF.createOrReplaceTempView("records")

    # Queries can then join DataFrame data with data stored in Hive.
    random_python_code.sql("SELECT g.user_id, h.event_type FROM users g JOIN events h ON g.user_id = h.user_id WHERE g.country = 'USA' AND h.event_date >= '2024-01-01' AND h.event_type IN ('click', 'view') AND g.age < 60;")
