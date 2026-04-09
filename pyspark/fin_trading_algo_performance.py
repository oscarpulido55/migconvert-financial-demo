Converting a Python PySpark script to Java Spark involves translating the Spark DataFrame API calls, handling Python-specific libraries like `datetime` and `pytz` with their Java equivalents, and restructuring the code within a Java class with a `main` method.

Here's the Java Spark equivalent:

```java
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;

import java.time.LocalDateTime;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

// Import static methods for easier access to Spark functions
import static org.apache.spark.sql.functions.*;

public class AlgorithmicTradingPerformance {

    /**
     * Converts a local time (hour, minute, date string) in a specified timezone to a UTC timestamp string.
     *
     * @param hour     The hour component of the local time.
     * @param minute   The minute component of the local time.
     * @param dateStr  The date string in 'YYYY-MM-DD' format.
     * @param tz       The timezone string, e.g., 'America/New_York'.
     * @return A UTC timestamp string in 'YYYY-MM-DD HH:MM:SS' format.
     */
    public String localToUtcTime(int hour, int minute, String dateStr, String tz) {
        // Parse date string into LocalDate
        LocalDate dateObj = LocalDate.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE);
        // Combine date and time
        LocalDateTime localDateTime = LocalDateTime.of(dateObj, LocalTime.of(hour, minute));
        // Localize to the specified timezone
        ZonedDateTime zonedLocalTime = ZonedDateTime.of(localDateTime, ZoneId.of(tz));
        // Convert to UTC
        ZonedDateTime utcDateTime = zonedLocalTime.withZoneSameInstant(ZoneOffset.UTC);
        // Format to string
        return utcDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    /**
     * Helper method to rename columns by adding a prefix.
     *
     * @param df          The DataFrame to modify.
     * @param prefix      The prefix to add.
     * @param excludeCols A list of columns to exclude from renaming.
     * @return A new DataFrame with renamed columns.
     */
    private Dataset<Row> prefixColumns(Dataset<Row> df, String prefix, List<String> excludeCols) {
        Dataset<Row> resultDf = df;
        for (String colName : df.columns()) {
            if (!excludeCols.contains(colName)) {
                resultDf = resultDf.withColumnRenamed(colName, prefix + "_" + colName);
            }
        }
        return resultDf;
    }

    /**
     * Defines a closure-like function to find the nearest quote.
     * In Java, this is implemented as a private helper method.
     *
     * @param ordersDf           The DataFrame containing order information.
     * @param quotesDf           The DataFrame containing market quotes.
     * @param targetTimeCol      The name of the column in ordersDf that represents the target time for lookup.
     * @param lowerBoundCol      The name of the column in ordersDf that represents the lower bound for quote search.
     * @param prefix             The prefix to add to the renamed quote columns.
     * @return A DataFrame containing orders joined with their nearest quotes.
     */
    private Dataset<Row> findNearestQuote(Dataset<Row> ordersDf, Dataset<Row> quotesDf,
                                          String targetTimeCol, String lowerBoundCol, String prefix) {

        // Define join condition
        Column joinCond = ordersDf.col("ticker").equalTo(quotesDf.col("ticker"))
                .and(col("quote_timestamp").between(col(lowerBoundCol), col(targetTimeCol)));

        Dataset<Row> joined = ordersDf.join(quotesDf, joinCond, "inner")
                // No direct .hint("merge") in Java API as readily available in PySpark, rely on Spark optimizer
                .withColumn("time_diff", abs(col(targetTimeCol).minus(col("quote_timestamp"))));

        // Define window for finding nearest quote
        WindowSpec orderPkOrderByTimeDiff = Window.partitionBy("order_pk").orderBy("time_diff");

        Dataset<Row> nearest = joined
                .withColumn("rn", row_number().over(orderPkOrderByTimeDiff))
                .filter(col("rn").equalTo(1))
                // Drop temporary columns and original quote ticker (to avoid ambiguity)
                .drop("rn", "time_diff", "Start_lower", "End_lower", "End_plus1", "End_plus1_lower",
                      "End_plus5", "End_plus5_lower", quotesDf.col("ticker")); // Ensure to drop 'ticker' from quotesDf explicitly

        // Rename quote columns uniquely
        for (String c : Arrays.asList("quote_timestamp", "best_bid", "best_ask")) {
            nearest = nearest.withColumnRenamed(c, prefix + "_" + c);
        }
        return nearest;
    }


    /**
     * Executes the algorithmic trading performance evaluation pipeline.
     *
     * @param spark    The SparkSession instance.
     * @param runDate  The date for which to run the analysis, e.g., "2023-10-26".
     */
    public void executePipeline(SparkSession spark, String runDate) {

        System.out.println("Starting performance analysis for date: " + runDate);

        // 1. Load Core Datasets
        Dataset<Row> allOrderEventsDf = spark.read().parquet("hdfs://trading_events_base/");
        Dataset<Row> parentOrdersDf = spark.read().parquet("hdfs://parent_orders/");

        // 2. Separate Event Stream into Fills
        // Anonymized protocol filtering conceptually representing status flags
        List<String> fillStatuses = Arrays.asList("FILLED", "PARTIAL");
        Dataset<Row> fillsDf = allOrderEventsDf.filter(
                (col("protocol_version").equalTo("V1")
                        .and(col("status").isin(fillStatuses.toArray(new String[0])))
                        .and(col("event_type").equalTo("TRADE")))
                        .or(col("protocol_version").geq("V2")
                                .and(col("event_type").equalTo("FILL")))
        );

        // Get the closing fill price per order
        WindowSpec orderClientIdAscTimestampWindow = Window.partitionBy("order_id", "client_id").orderBy("event_timestamp");
        Dataset<Row> closeFillPriceDf = fillsDf
                .withColumn("rn", row_number().over(orderClientIdAscTimestampWindow))
                .filter(col("rn").equalTo(1))
                .select(
                        col("order_id").as("close_order_id"),
                        col("client_id").as("close_client_id"),
                        col("last_exec_price").as("closing_price"),
                        col("last_exec_qty").as("closing_qty")
                )
                .drop("rn");

        // Aggregate fills per order
        Dataset<Row> fillsAggDf = fillsDf
                .groupBy("order_id", "client_id", "trade_date")
                .agg(
                        least(min("event_timestamp"), min("routing_timestamp")).as("FillStartTime"),
                        min("event_timestamp").as("FirstFillTime"),
                        sum("last_exec_qty").as("TotalSharesExecuted"),
                        count("last_exec_qty").as("NumberOfFills"),
                        (sum(col("last_exec_qty").multiply(col("last_exec_price")))
                                .divide(sum("last_exec_qty"))).as("AverageExecutionPrice"),
                        sum(col("last_exec_qty").multiply(col("last_exec_price"))).as("TotalMarketValueExecuted")
                );

        // Prefix columns for joins
        fillsAggDf = prefixColumns(fillsAggDf, "fill", Arrays.asList("order_id", "client_id", "trade_date"));

        // 3. Execution Acknowledgements
        List<String> ackStatuses = Arrays.asList("NEW", "REPLACED");
        Dataset<Row> acksDf = allOrderEventsDf.filter(col("status").isin(ackStatuses.toArray(new String[0])).and(col("event_type").equalTo("ACK")));
        Dataset<Row> acksAggDf = acksDf
                .groupBy("order_id", "client_id", "trade_date")
                .agg(least(min("event_timestamp"), min("routing_timestamp")).as("AckStartTime"));

        acksAggDf = prefixColumns(acksAggDf, "ack", Arrays.asList("order_id", "client_id", "trade_date"));

        // 4. Execution Terminations
        List<String> terminationStatuses = Arrays.asList("CANCELED", "DONE_FOR_DAY", "EXPIRED", "REJECTED");
        Dataset<Row> terminationsDf = allOrderEventsDf.filter(col("status").isin(terminationStatuses.toArray(new String[0])));
        Dataset<Row> terminationsAggDf = terminationsDf
                .groupBy("order_id", "client_id", "trade_date")
                .agg(greatest(max("event_timestamp"), max("routing_timestamp")).as("ExecutionEndTime"));

        terminationsAggDf = prefixColumns(terminationsAggDf, "term", Arrays.asList("order_id", "client_id", "trade_date"));

        // 5. Bring it back to Parent Orders
        // Define common join columns for convenience
        Column orderClientJoin = col("p.order_id").equalTo(col("order_id")).and(col("p.client_id").equalTo(col("client_id")));

        Dataset<Row> enrichedOrdersDf = parentOrdersDf.alias("p")
                .join(fillsAggDf.alias("f"),
                        col("p.order_id").equalTo(col("f.order_id")).and(col("p.client_id").equalTo(col("f.client_id"))),
                        "left")
                .join(acksAggDf.alias("a"),
                        col("p.order_id").equalTo(col("a.order_id")).and(col("p.client_id").equalTo(col("a.client_id"))),
                        "left")
                .join(terminationsAggDf.alias("t"),
                        col("p.order_id").equalTo(col("t.order_id")).and(col("p.client_id").equalTo(col("t.client_id"))),
                        "left")
                .join(closeFillPriceDf.alias("c"),
                        col("p.order_id").equalTo(col("c.close_order_id")).and(col("p.client_id").equalTo(col("c.close_client_id"))),
                        "left")
                .drop(col("f.order_id"), col("f.client_id"),
                        col("a.order_id"), col("a.client_id"),
                        col("t.order_id"), col("t.client_id"),
                        col("c.close_order_id"), col("c.close_client_id"));

        // Convert local market open/close times to UTC for comparison
        String utcTimeMarketOpen = localToUtcTime(9, 30, runDate, "America/New_York");
        String utcTimeMarketClose = localToUtcTime(16, 0, runDate, "America/New_York");

        // Establish effective operating window
        enrichedOrdersDf = enrichedOrdersDf
                .withColumn("EffectiveStartTime",
                        least(
                                greatest(col("ack_AckStartTime"), lit(utcTimeMarketOpen).cast(DataTypes.TimestampType)),
                                col("fill_FillStartTime")
                        )
                )
                .withColumn("EffectiveEndTime",
                        least(col("term_ExecutionEndTime"), lit(utcTimeMarketClose).cast(DataTypes.TimestampType))
                );

        // 6. Market Data Tick Metrics (complex time-based joins)
        Dataset<Row> quotesDf = spark.read().parquet("hdfs://level1_quotes/");

        quotesDf = quotesDf.repartition(col("ticker")).sortWithinPartitions("quote_timestamp");
        // enrichedOrdersDf should already be sorted for better join performance after adding timestamps if needed.
        // For range joins, sortWithinPartitions can be helpful but not strictly necessary for unique keys here.
        // It's usually applied *before* the join if the join key is part of the sort.
        // Re-partitioning parent_orders_df before `enriched_orders_df` is constructed could be better
        // but given the python script it is applied to enriched_orders_df right before the `find_nearest_quote` helper.
        enrichedOrdersDf = enrichedOrdersDf.repartition(col("ticker")).sortWithinPartitions("EffectiveStartTime");


        quotesDf = quotesDf.withColumn("quote_pk", monotonically_increasing_id());
        enrichedOrdersDf = enrichedOrdersDf.withColumn("order_pk", monotonically_increasing_id());

        // Build Window buffers for Quote lookups using Spark SQL expressions for intervals
        enrichedOrdersDf = enrichedOrdersDf
                .withColumn("Start_lower", expr("EffectiveStartTime - interval 10 minutes"))
                .withColumn("End_lower", expr("EffectiveEndTime - interval 10 minutes"))
                .withColumn("End_plus1", expr("EffectiveEndTime + interval 1 minute"))
                .withColumn("End_plus1_lower", expr("End_plus1 - interval 10 minutes"))
                .withColumn("End_plus5", expr("EffectiveEndTime + interval 5 minutes"))
                .withColumn("End_plus5_lower", expr("End_plus5 - interval 10 minutes"));

        // Perform lookups using the helper method
        Dataset<Row> openQuotes = findNearestQuote(enrichedOrdersDf, quotesDf, "EffectiveStartTime", "Start_lower", "start");
        Dataset<Row> endQuotes = findNearestQuote(enrichedOrdersDf, quotesDf, "EffectiveEndTime", "End_lower", "end");
        Dataset<Row> end1mQuotes = findNearestQuote(enrichedOrdersDf, quotesDf, "End_plus1", "End_plus1_lower", "end_plus1");
        // No end_5m_quotes in Python, so not adding here

        // 7. Core VWAP and Financial Performance calculations
        Dataset<Row> tradesDf = spark.read().parquet("hdfs://market_trades/");

        // VWAP during order existence
        Dataset<Row> vwapDf = enrichedOrdersDf
                .join(tradesDf,
                        enrichedOrdersDf.col("ticker").equalTo(tradesDf.col("ticker"))
                                .and(tradesDf.col("trade_timestamp").geq(enrichedOrdersDf.col("EffectiveStartTime")))
                                .and(tradesDf.col("trade_timestamp").leq(enrichedOrdersDf.col("EffectiveEndTime"))),
                        "inner")
                .groupBy(enrichedOrdersDf.col("order_id"), enrichedOrdersDf.col("client_id")) // specify which order_id/client_id to group by
                .agg(
                        sum(tradesDf.col("trade_size")).as("market_interval_volume"),
                        (sum(tradesDf.col("trade_price").multiply(tradesDf.col("trade_size")))
                                .divide(sum(tradesDf.col("trade_size")))).as("market_interval_vwap")
                );

        // Join everything back
        Dataset<Row> finalDf = enrichedOrdersDf
                .join(vwapDf, new Column[]{col("order_id"), col("client_id")}, "left")
                // Selectively bringing fields from quote buffers
                .join(openQuotes.select("order_pk", "start_best_bid", "start_best_ask", "start_quote_timestamp"),
                      new Column[]{col("order_pk")}, "left")
                .join(endQuotes.select("order_pk", "end_best_bid", "end_best_ask"),
                      new Column[]{col("order_pk")}, "left")
                .join(end1mQuotes.select("order_pk", "end_plus1_best_bid", "end_plus1_best_ask"),
                      new Column[]{col("order_pk")}, "left")
                .drop("order_pk"); // drop the temporary primary key

        // Calculate complex performance metrics (Slippage, Momentum, Profit/Loss vectors)
        finalDf = finalDf.withColumn("arrival_mid_price",
                (col("start_best_bid").plus(col("start_best_ask"))).divide(2));

        // Slippage from VWAP
        finalDf = finalDf.withColumn("slippage_from_vwap_bps",
                when(col("side").equalTo("BUY"),
                        (col("fill_AverageExecutionPrice").minus(col("market_interval_vwap")))
                                .divide(col("market_interval_vwap")).multiply(10000))
                        .when(col("side").equalTo("SELL"),
                                (col("market_interval_vwap").minus(col("fill_AverageExecutionPrice")))
                                        .divide(col("market_interval_vwap")).multiply(10000))
                        .otherwise(lit(null).cast(DataTypes.DoubleType)) // handle null for cases not BUY/SELL
        );

        // Slippage from Arrival Mid (Implementation Shortfall)
        finalDf = finalDf.withColumn("implementation_shortfall_pl",
                when(col("side").equalTo("BUY"),
                        (col("arrival_mid_price").minus(col("fill_AverageExecutionPrice")))
                                .multiply(col("fill_TotalSharesExecuted")))
                        .when(col("side").equalTo("SELL"),
                                (col("fill_AverageExecutionPrice").minus(col("arrival_mid_price")))
                                        .multiply(col("fill_TotalSharesExecuted")))
                        .otherwise(lit(null).cast(DataTypes.DoubleType))
        );

        // Momentum calculations post-trade
        finalDf = finalDf.withColumn("post_trade_1m_momentum",
                when(col("side").equalTo("BUY"),
                        (((col("end_plus1_best_bid").plus(col("end_plus1_best_ask"))).divide(2))
                                .minus((col("end_best_bid").plus(col("end_best_ask"))).divide(2)))
                                .multiply(col("fill_TotalSharesExecuted")))
                        .when(col("side").equalTo("SELL"),
                                (((col("end_best_bid").plus(col("end_best_ask"))).divide(2))
                                        .minus((col("end_plus1_best_bid").plus(col("end_plus1_best_ask"))).divide(2)))
                                        .multiply(col("fill_TotalSharesExecuted")))
                        .otherwise(lit(null).cast(DataTypes.DoubleType))
        );

        finalDf = finalDf.withColumn("opportunity_cost_pl",
                when(col("side").equalTo("BUY"),
                        (col("requested_shares").minus(col("fill_TotalSharesExecuted")))
                                .multiply(col("fill_AverageExecutionPrice").minus(col("closing_price"))))
                        .when(col("side").equalTo("SELL"),
                                (col("requested_shares").minus(col("fill_TotalSharesExecuted")))
                                        .multiply(col("closing_price").minus(col("fill_AverageExecutionPrice"))))
                        .otherwise(lit(null).cast(DataTypes.DoubleType))
        );

        // Output results
        String outputPath = String.format("hdfs://trading_analytics/run_date=%s", runDate);
        finalDf.write().mode("overwrite").parquet(outputPath);

        System.out.println("Performance analysis complete. Output written to: " + outputPath);
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: AlgorithmicTradingPerformance <run_date>");
            System.exit(1);
        }

        // Initialize SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("AlgorithmicTradingPerformance")
                .getOrCreate();

        AlgorithmicTradingPerformance app = new AlgorithmicTradingPerformance();
        app.executePipeline(spark, args[0]);

        spark.stop();
    }
}
```

### Key Changes and Explanations:

1.  **Imports:**
    *   `org.apache.spark.sql.*`: Core Spark SQL classes.
    *   `static org.apache.spark.sql.functions.*`: This is crucial! It allows you to call Spark SQL functions (like `col`, `lit`, `when`, `sum`, `min`, `max`, `row_number`, etc.) directly without prefixing `functions.`, making the code much cleaner and closer to PySpark's `F.` alias.
    *   `java.time.*`: Replaces Python's `datetime` and `pytz` for date/time handling. This includes `LocalDate`, `LocalDateTime`, `LocalTime`, `ZoneId`, `ZonedDateTime`, `ZoneOffset`, and `DateTimeFormatter`.
    *   `java.util.*`: For `List`, `Arrays`, etc.

2.  **`AlgorithmicTradingPerformance` Class:** The Python class is directly translated into a Java class.

3.  **`localToUtcTime` Method:**
    *   Python's `datetime.strptime`, `localize`, `astimezone`, `strftime` are replaced with `java.time` APIs:
        *   `LocalDate.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE)`: Parses the date string.
        *   `LocalDateTime.of(dateObj, LocalTime.of(hour, minute))`: Combines date and time.
        *   `ZonedDateTime.of(localDateTime, ZoneId.of(tz))`: Localizes to the specified timezone.
        *   `withZoneSameInstant(ZoneOffset.UTC)`: Converts to UTC.
        *   `format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))`: Formats the result.

4.  **`prefixColumns` Helper Method:** Because `withColumnRenamed` returns a new DataFrame, and `df.columns()` is a fixed array, directly modifying `df` in a loop like in Python is not as straightforward. This helper method makes the column renaming logic cleaner and reusable.

5.  **`findNearestQuote` Helper Method:** This replicates the `closure` concept in Python.
    *   It takes `Dataset<Row>` objects, column names, and a prefix.
    *   `ordersDf.col("ticker").equalTo(quotesDf.col("ticker"))`: When joining, it's good practice to explicitly state which DataFrame's column you're referring to, especially if column names are identical.
    *   There is no direct Java equivalent for `DataFrame.hint("merge")`. Spark's Catalyst optimizer generally handles join strategies well. If performance issues arise, you'd configure hints via `SparkSession` configurations (e.g., `spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")`) or use specific join APIs for broadcast joins if applicable. For a direct translation, it's often omitted.
    *   When dropping columns that might exist in both DataFrames after a join (like `ticker`), specify `quotesDf.col("ticker")` to avoid ambiguity.
    *   The `drop` method must explicitly list all columns to drop.

6.  **DataFrame Operations:**
    *   **Method Chaining:** PySpark's `df.method1().method2()` directly translates to Java.
    *   **Column Operations:**
        *   `F.col("name")` becomes `col("name")`.
        *   Arithmetic operations like `F.col("a") * F.col("b")` become `col("a").multiply(col("b"))`. Similarly, `.plus()`, `.minus()`, `.divide()`.
        *   `F.isin(...)` becomes `col("columnName").isin(Object...)`. Note it takes a variable argument list of `Object`s, so for a `List<String>`, you use `list.toArray(new String[0])`.
        *   `F.when(...).when(...).otherwise(...)` is identical. Ensure to cast `null` literals (`lit(null).cast(DataTypes.DoubleType)`) for consistent column types if branches result in different types.
    *   **Literals:** `F.lit("value")` becomes `lit("value")`.
    *   **Casting:** `F.col(...).cast("timestamp")` becomes `col(...).cast(DataTypes.TimestampType)`. `DataTypes` provides various Spark data types.
    *   **Window Functions:** `Window.partitionBy(...).orderBy(...)` is similar, just `WindowSpec` is the Java type.
    *   **SQL Expressions:** `F.expr(...)` is `expr(...)` for string-based SQL expressions.
    *   **`groupBy().agg()`:** The `agg` part is similar, using various aggregate functions.
    *   **Joins:** `df1.join(df2, join_condition, "left")`. When joining on multiple columns, `new Column[]{col("col1"), col("col2")}` is often used.

7.  **`main` Method:**
    *   Standard Java `main` method.
    *   Command-line arguments (`sys.argv[1]`) are accessed via `args[0]`.
    *   `System.err.println` for error messages.
    *   `System.exit(1)` for non-zero exit code.
    *   `spark.stop()` is important for proper resource cleanup.

### To Compile and Run this Java Spark Application:

1.  **Maven Project Setup:**
    Create a `pom.xml` file for a Maven project.

    ```xml
    <?xml version="1.0" encoding="UTF-8"?>
    <project xmlns="http://maven.apache.org/POM/4.0.0"
             xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
             xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
        <modelVersion>4.0.0</modelVersion>

        <groupId>com.example</groupId>
        <artifactId>algorithmic-trading-performance</artifactId>
        <version>1.0-SNAPSHOT</version>

        <properties>
            <maven.compiler.source>1.8</maven.compiler.source>
            <maven.compiler.target>1.8</maven.compiler.target>
            <spark.version>3.5.0</spark.version> <!-- Adjust Spark version as needed -->
        </properties>

        <dependencies>
            <!-- Spark SQL -->
            <dependency>
                <groupId>org.apache.spark</groupId>
                <artifactId>spark-sql_2.12</artifactId> <!-- Adjust Scala version if necessary, e.g., _2.11 -->
                <version>${spark.version}</version>
                <scope>provided</scope>
            </dependency>
            <!-- If you use `spark.sql.functions.from_unixtime` etc., `spark-core` might be implied by spark-sql,
                 but if you had any core spark usage explicitely add spark-core_2.12 -->
            <!-- For Hadoop client (HDFS paths), implicitly added by Spark dependencies but good to be aware -->
             <dependency>
                <groupId>org.apache.hadoop</groupId>
                <artifactId>hadoop-client</artifactId>
                <version>3.3.4</version> <!-- Adjust Hadoop client version as needed for your HDFS cluster -->
                <scope>provided</scope>
            </dependency>
        </dependencies>

        <build>
            <plugins>
                <!-- Plugin to compile Java code -->
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-compiler-plugin</artifactId>
                    <version>3.8.1</version>
                    <configuration>
                        <source>${maven.compiler.source}</source>
                        <target>${maven.compiler.target}</target>
                    </configuration>
                </plugin>
                <!-- Plugin to create a fat JAR (JAR with dependencies) -->
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-shade-plugin</artifactId>
                    <version>3.2.4</version>
                    <executions>
                        <execution>
                            <phase>package</phase>
                            <goals>
                                <goal>shade</goal>
                            </goals>
                            <configuration>
                                <createDependencyReducedPom>false</createDependencyReducedPom>
                                <transformers>
                                    <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                        <mainClass>AlgorithmicTradingPerformance</mainClass>
                                    </transformer>
                                </transformers>
                                <!-- Exclude Spark dependencies as they are provided by the cluster -->
                                <filters>
                                    <filter>
                                        <artifact>*:*</artifact>
                                        <excludes>
                                            <exclude>META-INF/*.SF</exclude>
                                            <exclude>META-INF/*.DSA</exclude>
                                            <exclude>META-INF/*.RSA</exclude>
                                        </excludes>
                                    </filter>
                                </filters>
                            </configuration>
                        </execution>
                    </executions>
                </plugin>
            </plugins>
        </build>
    </project>
    ```

2.  **Compile:**
    Navigate to your project directory (where `pom.xml` is) and run:
    ```bash
    mvn clean package
    ```
    This will create a `target/algorithmic-trading-performance-1.0-SNAPSHOT.jar` (or similar) which is a "fat JAR" containing your code and its dependencies (excluding Spark, which is `provided` by the cluster).

3.  **Run on Spark Cluster:**
    ```bash
    spark-submit \
      --class AlgorithmicTradingPerformance \
      --master yarn \
      --deploy-mode client \
      target/algorithmic-trading-performance-1.0-SNAPSHOT.jar \
      "2023-10-26"
    ```
    *   `--class`: Specifies the main class to run.
    *   `--master yarn`: Use YARN as the resource manager (adjust if using Standalone, Mesos, K8s, etc.).
    *   `--deploy-mode client`: Run the driver on the client machine (can be `cluster` for production).
    *   `target/algorithmic-trading-performance-1.0-SNAPSHOT.jar`: The path to your compiled JAR.
    *   `"2023-10-26"`: The `run_date` argument passed to your `main` method.

Remember to replace HDFS paths and Spark/Hadoop versions with those appropriate for your environment.