Okay, let's convert your PySpark script to a Java Spark application.

Key considerations for this conversion:
1.  **SparkSession Setup:** Similar in Java.
2.  **UDFs:** Java UDFs require implementing `org.apache.spark.sql.api.java.UDF` interfaces (e.g., `UDF4` for 4 arguments) and then registering them with `spark.udf().register()`.
3.  **DataFrame API:** Most `pyspark.sql.functions` have direct Java equivalents in `org.apache.spark.sql.functions`.
    *   `col("name")` becomes `functions.col("name")`.
    *   `lit(value)` becomes `functions.lit(value)`.
    *   `df.alias("t")` becomes `df.as("t")`.
    *   Arithmetic operations like `+`, `-`, `/` are methods on `Column` objects: `col("a").plus(col("b"))`, `col("a").minus(col("b"))`, `col("a").divide(col("b"))`.
    *   Comparisons: `equalTo()`, `gt()`, `geq()`, `lt()`, `leq()`, `notEqual()`.
    *   Logical operators: `and()`, `or()`.
    *   `udf(...)` call in PySpark becomes `callUDF("udf_name", col1, col2, ...)` in Java.
    *   `fillna` becomes `df.na().fill(value, new String[]{"col1", "col2"})`.
    *   Window functions: `org.apache.spark.sql.expressions.Window` is similar. `rangeBetween(-3600, 0)` needs `L` suffix for `long` (e.g., `rangeBetween(-3600L, 0L)`).
4.  **Main Execution:** Standard Java `public static void main(String[] args)` method.
5.  **Imports:** Need to import relevant Spark classes and `static org.apache.spark.sql.functions.*` to avoid prefixing `functions.` everywhere.
6.  **Maven/Gradle:** You'll need to set up a `pom.xml` (Maven) or `build.gradle` (Gradle) for your project to include Spark dependencies.

---

Here's the Java Spark code:

```java
package com.example.spark; // You can choose your own package name

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class FinancialCreditCardFraudScoring {

    /**
     * Initializes and returns a Spark session with Hive Metastore support.
     * @return SparkSession instance
     */
    private static SparkSession createSparkSession() {
        return SparkSession.builder()
                .appName("Financial_Credit_Card_Fraud_Scoring")
                .enableHiveSupport()
                .getOrCreate();
    }

    // Custom UDF for great circle distance
    // Implementing UDF4 for 4 Double arguments and returning a Double
    private static final UDF4<Double, Double, Double, Double, Double> HAVERSINE_UDF =
        new UDF4<Double, Double, Double, Double, Double>() {
            @Override
            public Double call(Double lat1, Double lon1, Double lat2, Double lon2) throws Exception {
                if (lat1 == null || lon1 == null || lat2 == null || lon2 == null) {
                    return -1.0;
                }
                double R = 6371.0; // Radius of earth in km
                double phi1 = Math.toRadians(lat1);
                double phi2 = Math.toRadians(lat2);
                double dphi = Math.toRadians(lat2 - lat1);
                double dlambda = Math.toRadians(lon2 - lon1);

                double a = Math.sin(dphi / 2) * Math.sin(dphi / 2) +
                           Math.cos(phi1) * Math.cos(phi2) *
                           Math.sin(dlambda / 2) * Math.sin(dlambda / 2);
                return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
            }
        };

    /**
     * Uses pure Spark DataFrame APIs to score credit card transactions.
     * Replaces embedded SQL with continuous DataFrame transformations.
     * @param spark The SparkSession object.
     * @param executionDate The date for which to process transactions (YYYY-MM-DD).
     */
    private static void scoreTransactionsForFraud(SparkSession spark, String executionDate) {
        // Register the Haversine UDF
        spark.udf().register("haversine_udf", HAVERSINE_UDF, DataTypes.DoubleType);

        System.out.println("Starting fraud scoring for date: " + executionDate);

        // 1. Load Data
        Dataset<Row> fullCcTrx = spark.table("fin_core.cc_transactions");
        Dataset<Row> accounts = spark.table("fin_core.dim_accounts");
        Dataset<Row> merchants = spark.table("fin_core.dim_merchants");

        // 2. Extract current day transactions
        Dataset<Row> ccTrx = fullCcTrx.filter(col("trx_date").equalTo(executionDate));
        System.out.println("Current day transactions loaded: " + ccTrx.count());

        // 3. Join location data and calculate distance Native
        Dataset<Row> enrichedTrx = ccTrx.as("t")
                .join(accounts.as("a"), col("t.account_id").equalTo(col("a.account_id")), "inner")
                .join(merchants.as("m"), col("t.merchant_id").equalTo(col("m.merchant_id")), "left")
                .withColumn(
                    "distance_from_home_km",
                    callUDF("haversine_udf",
                            col("a.home_lat"), col("a.home_lon"),
                            col("m.merchant_lat"), col("m.merchant_lon"))
                );
        System.out.println("Transactions enriched with distance: " + enrichedTrx.count());


        // 4. Pure DataFrame Historical Profiling Window
        // Filter for the last 90 days of transactions (excluding execution date)
        Column histTrxDateFilter = col("trx_date").geq(date_sub(lit(executionDate), 90))
                                 .and(col("trx_date").leq(date_sub(lit(executionDate), 1)));
        Dataset<Row> histTrx = fullCcTrx.filter(histTrxDateFilter);

        // Aggregate to build the historical profile
        Dataset<Row> histProfile = histTrx.groupBy(col("account_id"))
                .agg(
                    avg(col("amount")).as("avg_trx_amount_90d"),
                    stddev_samp(col("amount")).as("stddev_trx_amount_90d"),
                    (count(col("trx_id")).divide(lit(90.0))).as("avg_daily_trx_count") // Ensure double division
                )
                // fillna is handled via na().fill() for DataFrames
                .na().fill(0.0, new String[]{"stddev_trx_amount_90d"});
        System.out.println("Historical profiles generated for " + histProfile.count() + " accounts.");


        // 5. Join current day transactions with their historical profiles
        Dataset<Row> dfFeatures = enrichedTrx.as("curr")
                .join(histProfile.as("hist"), col("curr.account_id").equalTo(col("hist.account_id")), "left");
        System.out.println("Features DataFrame created: " + dfFeatures.count());


        // 6. Apply Time-based Window Function (Last Hour Trx Count)
        // unix_timestamp is crucial for rangeBetween to work on actual time, not row count
        WindowSpec timeWindow = Window
                .partitionBy(col("curr.account_id"))
                .orderBy(unix_timestamp(col("curr.trx_timestamp"))) // Expects a timestamp string
                .rangeBetween(-3600L, 0L); // -3600 seconds (1 hour ago) to current row (0 seconds ago)


        // 7. Apply Complex Business Logic and Scoring Native DataFrame API
        Dataset<Row> scoredDf = dfFeatures
                .withColumn("trx_last_hour_cnt", count(col("curr.trx_id")).over(timeWindow))
                .withColumn("amount_z_score",
                    when(col("hist.stddev_trx_amount_90d").gt(lit(0.0)),
                         (col("curr.amount").minus(col("hist.avg_trx_amount_90d"))).divide(col("hist.stddev_trx_amount_90d")))
                    .otherwise(lit(0.0))
                )
                .withColumn("distance_risk_score",
                    when(col("distance_from_home_km").gt(lit(500)).and(col("distance_from_home_km").notEqual(lit(-1.0))), lit(30))
                    .otherwise(lit(0))
                )
                .withColumn("amount_risk_score",
                    when(col("amount_z_score").gt(lit(3.0)), lit(40))
                    .when(col("amount_z_score").gt(lit(2.0)), lit(20))
                    .otherwise(lit(0))
                )
                .withColumn("velocity_risk_score",
                    when(col("trx_last_hour_cnt").gt(lit(5)), lit(30))
                    .when(col("trx_last_hour_cnt").gt(lit(3)), lit(15))
                    .otherwise(lit(0))
                )
                .withColumn("fraud_score",
                    col("distance_risk_score").plus(col("amount_risk_score")).plus(col("velocity_risk_score"))
                )
                .withColumn("is_fraud_alert", col("fraud_score").geq(lit(60)));
        System.out.println("Transactions scored: " + scoredDf.count());


        // 8. Select final columns and write to target
        Dataset<Row> finalOutput = scoredDf.select(
            col("curr.trx_id"), col("curr.account_id"), col("curr.amount"),
            col("distance_from_home_km"), col("amount_z_score"), col("trx_last_hour_cnt"),
            col("fraud_score"), col("is_fraud_alert"), lit(executionDate).as("scoring_date")
        );

        finalOutput.write()
                .mode("append")
                .insertInto("fin_mart.fraud_scores_daily");
        
        System.out.println("Pure DataFrame Fraud scoring completed for " + executionDate);
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: spark-submit --class com.example.spark.FinancialCreditCardFraudScoring your-jar-file.jar <YYYY-MM-DD>");
            System.exit(1);
        }

        String executionDate = args[0];
        SparkSession spark = createSparkSession();

        try {
            scoreTransactionsForFraud(spark, executionDate);
        } finally {
            spark.stop();
        }
    }
}
```

---

### To Compile and Run this Java Spark Application:

1.  **Project Setup (Maven Example):**
    Create a `pom.xml` file for your Maven project.
    Replace `3.x.x` with your actual Spark version (e.g., `3.4.0` or `3.5.0`).
    Adjust the Scala version (e.g., `2.12`) to match your Spark distribution.

    ```xml
    <?xml version="1.0" encoding="UTF-8"?>
    <project xmlns="http://maven.apache.org/POM/4.0.0"
             xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
             xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
        <modelVersion>4.0.0</modelVersion>

        <groupId>com.example.spark</groupId>
        <artifactId>financial-fraud-scoring</artifactId>
        <version>1.0</version>

        <properties>
            <maven.compiler.source>1.8</maven.compiler.source>
            <maven.compiler.target>1.8</maven.compiler.target>
            <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
            <spark.version>3.4.0</spark.version> <!-- !!! IMPORTANT: Update to your Spark version !!! -->
            <scala.version.suffix>2.12</scala.version.suffix> <!-- !!! IMPORTANT: Update to your Scala version suffix, e.g., 2.11 or 2.12 !!! -->
        </properties>

        <dependencies>
            <!-- Spark Core -->
            <dependency>
                <groupId>org.apache.spark</groupId>
                <artifactId>spark-core_${scala.version.suffix}</artifactId>
                <version>${spark.version}</version>
                <scope>provided</scope>
            </dependency>
            <!-- Spark SQL -->
            <dependency>
                <groupId>org.apache.spark</groupId>
                <artifactId>spark-sql_${scala.version.suffix}</artifactId>
                <version>${spark.version}</version>
                <scope>provided</scope>
            </dependency>
            <!-- Spark Hive (for enableHiveSupport() and spark.table() to work) -->
            <dependency>
                <groupId>org.apache.spark</groupId>
                <artifactId>spark-hive_${scala.version.suffix}</artifactId>
                <version>${spark.version}</version>
                <scope>provided</scope>
            </dependency>
        </dependencies>

        <build>
            <plugins>
                <!-- Plugin to make an executable JAR including dependencies -->
                <plugin>
                    <artifactId>maven-assembly-plugin</artifactId>
                    <version>3.3.0</version>
                    <configuration>
                        <archive>
                            <manifest>
                                <mainClass>com.example.spark.FinancialCreditCardFraudScoring</mainClass>
                            </manifest>
                        </archive>
                        <descriptorRefs>
                            <descriptorRef>jar-with-dependencies</descriptorRef>
                        </descriptorRefs>
                    </configuration>
                    <executions>
                        <execution>
                            <id>make-assembly</id> <!-- this is used for inheritance merges -->
                            <phase>package</phase> <!-- bind to the packaging phase -->
                            <goals>
                                <goal>single</goal>
                            </goals>
                        </execution>
                    </executions>
                </plugin>
            </plugins>
        </build>
    </project>
    ```

2.  **Compile the Project:**
    Navigate to your project root (where `pom.xml` is located) in your terminal and run:
    ```bash
    mvn clean package
    ```
    This will create a JAR file (e.g., `financial-fraud-scoring-1.0-jar-with-dependencies.jar`) in the `target/` directory.

3.  **Run with `spark-submit`:**
    ```bash
    spark-submit \
      --class com.example.spark.FinancialCreditCardFraudScoring \
      --master yarn \
      --deploy-mode client \
      --conf "spark.sql.warehouse.dir=/user/hive/warehouse" \
      --conf "spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:mysql://your_mysql_host:3306/your_hive_metastore_db?createDatabaseIfNotExist=true" \
      --conf "spark.hadoop.javax.jdo.option.ConnectionDriverName=com.mysql.cj.jdbc.Driver" \
      --conf "spark.hadoop.javax.jdo.option.ConnectionUserName=hiveuser" \
      --conf "spark.hadoop.javax.jdo.option.ConnectionPassword=hivepassword" \
      --jars /path/to/mysql-connector-java-x.x.xx.jar \
      target/financial-fraud-scoring-1.0-jar-with-dependencies.jar \
      "2023-10-26" # Replace with your desired execution date
    ```

    **Important Notes for `spark-submit`:**
    *   **`--class`**: Must match the full package and class name (`com.example.spark.FinancialCreditCardFraudScoring`).
    *   **`--master`**: Specify your Spark cluster manager (e.g., `yarn`, `k8s`, `local[*]`). For local testing, use `local[*]`.
    *   **`--deploy-mode`**: `client` or `cluster`.
    *   **Hive Metastore Configuration**: The `spark.hadoop.javax.jdo.option.ConnectionURL` and related properties are crucial for `enableHiveSupport()` to connect to your Hive Metastore. You might need to adjust these paths and credentials. If you're using a local Derby metastore (default for `enableHiveSupport()` if no external is configured), you might not need these.
    *   **MySQL Connector Jar:** If your Hive Metastore uses MySQL, you'll need to include the MySQL JDBC driver JAR using `--jars`. Make sure it's available to your Spark driver and executors.
    *   **Arguments**: `YYYY-MM-DD` is passed as a command-line argument.