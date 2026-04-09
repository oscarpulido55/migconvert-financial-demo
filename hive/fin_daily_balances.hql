Converting a complex HQL script with DDL (Data Definition Language) and DML (Data Manipulation Language) into a Java application involves several key steps:

1.  **Project Setup:** Create a Maven or Gradle project.
2.  **Dependencies:** Add Hive JDBC driver, SLF4J for logging.
3.  **Configuration:** Externalize database connection details and process date.
4.  **JDBC Connection:** Establish a connection to Hive using JDBC.
5.  **SQL Execution:** Execute the HQL statements using `java.sql.Statement` and `java.sql.PreparedStatement`.
6.  **Date Handling:** Replace hardcoded dates with dynamic Java date logic and parameterized SQL.
7.  **Error Handling & Logging:** Implement robust error handling and informative logging.

Here's the Java code, structured as a Maven project, with a `config.properties` file:

---

### 1. Maven `pom.xml`

This defines the project and its dependencies.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.example</groupId>
    <artifactId>hive-daily-balances</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>11</maven.compiler.source>
        <maven.compiler.target>11</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <hive.jdbc.version>3.1.2</hive.jdbc.version> <!-- Use the version compatible with your Hive installation -->
        <slf4j.version>1.7.30</slf4j.version>
        <logback.version>1.2.3</logback.version>
    </properties>

    <dependencies>
        <!-- Hive JDBC Driver -->
        <dependency>
            <groupId>org.apache.hive</groupId>
            <artifactId>hive-jdbc</artifactId>
            <version>${hive.jdbc.version}</version>
            <!-- Exclude these if they cause conflicts with your Hadoop environment -->
            <exclusions>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>slf4j-log4j12</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
            </exclusions>
        </dependency>

        <!-- Logging -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>${slf4j.version}</version>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <version>${logback.version}</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <!-- Plugin to build an executable JAR with all dependencies -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-assembly-plugin</artifactId>
                <version>3.3.0</version>
                <configuration>
                    <archive>
                        <manifest>
                            <mainClass>com.example.processor.DailyBalanceProcessor</mainClass>
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

---

### 2. `src/main/resources/config.properties`

This file will hold your Hive connection details and the date to process.

```properties
# Hive JDBC Connection Details
hive.jdbc.driver=org.apache.hive.jdbc.HiveDriver
hive.jdbc.url=jdbc:hive2://localhost:10000/default # Replace with your HiveServer2 URL
hive.jdbc.user=hiveuser # Replace with your Hive username
hive.jdbc.password= # Replace with your Hive password (leave blank if none)

# Processing Date
# This date will be used as the 'today's date' for balance calculation.
# Format: YYYY-MM-DD
processing.date=2024-01-01
```

---

### 3. `src/main/java/com/example/processor/DailyBalanceProcessor.java`

This is the main Java class that orchestrates the HQL execution.

```java
package com.example.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class DailyBalanceProcessor {

    private static final Logger logger = LoggerFactory.getLogger(DailyBalanceProcessor.class);

    private final Properties config;
    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;
    private final String hiveDriver;

    public DailyBalanceProcessor(Properties config) {
        this.config = config;
        this.hiveDriver = config.getProperty("hive.jdbc.driver");
        this.jdbcUrl = config.getProperty("hive.jdbc.url");
        this.jdbcUser = config.getProperty("hive.jdbc.user");
        this.jdbcPassword = config.getProperty("hive.jdbc.password");

        // Load Hive JDBC driver
        try {
            Class.forName(hiveDriver);
            logger.info("Successfully loaded Hive JDBC driver: {}", hiveDriver);
        } catch (ClassNotFoundException e) {
            logger.error("Hive JDBC driver not found: {}", hiveDriver, e);
            throw new RuntimeException("Failed to load Hive JDBC driver", e);
        }
    }

    private Connection getConnection() throws SQLException {
        logger.debug("Attempting to connect to Hive: {}", jdbcUrl);
        return DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
    }

    /**
     * Executes a DDL statement (e.g., CREATE TABLE, DROP TABLE, CREATE DATABASE).
     */
    private void executeDdl(Connection conn, String sql) throws SQLException {
        logger.info("Executing DDL: \n{}", sql);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            logger.info("DDL executed successfully.");
        }
    }

    /**
     * Executes a DML statement with parameters (e.g., INSERT).
     * @param conn The database connection.
     * @param sql The SQL query with '?' placeholders.
     * @param params Varargs for setting PreparedStatement parameters.
     * @throws SQLException
     */
    private void executeDml(Connection conn, String sql, String... params) throws SQLException {
        logger.info("Executing DML: \n{}", sql);
        logger.info("With parameters: {}", String.join(", ", params));
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                pstmt.setString(i + 1, params[i]);
            }
            pstmt.execute();
            logger.info("DML executed successfully.");
        }
    }

    /**
     * Generates the SQL for creating the fact_daily_balances table.
     */
    private String getCreateDailyBalancesTableSql() {
        return """
            CREATE TABLE IF NOT EXISTS fin_core.fact_daily_balances (
                account_id STRING COMMENT 'Unique identifier for the account',
                customer_id STRING COMMENT 'Identifier for the account owner',
                account_type STRING COMMENT 'Type of account (CHECKING, SAVINGS, LOAN)',
                open_date DATE COMMENT 'Date the account was opened',
                currency_code STRING COMMENT 'Base currency of the account',
                beginning_balance DECIMAL(18, 4) COMMENT 'Balance at the start of the day',
                total_credits DECIMAL(18, 4) COMMENT 'Total value of incoming funds',
                total_debits DECIMAL(18, 4) COMMENT 'Total value of outgoing funds',
                ending_balance DECIMAL(18, 4) COMMENT 'Balance at the end of the day',
                interest_accrued DECIMAL(18, 4) COMMENT 'Daily interest accrued',
                is_overdrawn BOOLEAN COMMENT 'Flag indicating if the account is in negative balance',
                etl_timestamp TIMESTAMP
            )
            COMMENT 'Stores the End of Day balances for all accounts'
            PARTITIONED BY (balance_date DATE, region_code STRING)
            STORED AS ORC
            TBLPROPERTIES ('transactional'='true');
            """;
    }

    /**
     * Generates the SQL for creating and populating the temporary daily movements table.
     * Parameters: processingDate (for trx_date in fact_transactions)
     */
    private String getCreateTempDailyMovementsSql() {
        // Note: We use '?' placeholders for the processing date.
        return """
            CREATE TEMPORARY TABLE default.tmp_daily_movements_stg AS
            WITH credited AS (
                SELECT
                    destination_account_id AS account_id,
                    SUM(amount_base_currency) AS total_credits
                FROM fin_core.fact_transactions
                WHERE trx_date = ?
                  AND transaction_type NOT IN ('FEE', 'REVERSAL_DEBIT')
                GROUP BY destination_account_id
            ),
            debited AS (
                SELECT
                    source_account_id AS account_id,
                    SUM(amount_base_currency) AS total_debits
                FROM fin_core.fact_transactions
                WHERE trx_date = ?
                  AND transaction_type NOT IN ('REVERSAL_CREDIT')
                GROUP BY source_account_id
            )
            SELECT
                COALESCE(c.account_id, d.account_id) AS account_id,
                COALESCE(c.total_credits, 0.0) AS total_credits,
                COALESCE(d.total_debits, 0.0) AS total_debits
            FROM credited c
            FULL OUTER JOIN debited d ON c.account_id = d.account_id;
            """;
    }

    /**
     * Generates the SQL for inserting/overwriting daily balances.
     * Parameters: previousDate (for prev.balance_date), processingDate (for balance_date partition)
     */
    private String getInsertDailyBalancesSql() {
        return """
            INSERT OVERWRITE TABLE fin_core.fact_daily_balances PARTITION (balance_date, region_code)
            SELECT
                a.account_id,
                a.customer_id,
                a.account_type,
                a.open_date,
                a.currency_code,

                -- Beginning Balance is the previous day's ending balance, or 0 if new
                COALESCE(prev.ending_balance, 0.0) AS beginning_balance,

                -- Daily Movements
                COALESCE(m.total_credits, 0.0) AS total_credits,
                COALESCE(m.total_debits, 0.0) AS total_debits,

                -- Ending Balance Calculation
                (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) AS ending_balance,

                -- Simple interest calculation (mock logic for demo: 0.05% APY / 365)
                CASE
                    WHEN a.account_type = 'SAVINGS' AND (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) > 0
                    THEN (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) * (0.05 / 365)
                    ELSE 0.0
                END AS interest_accrued,

                -- Overdrawn flag
                CASE
                    WHEN (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) < 0
                    THEN true
                    ELSE false
                END AS is_overdrawn,

                CURRENT_TIMESTAMP() AS etl_timestamp,

                -- Partition columns
                CAST(? AS DATE) AS balance_date, -- Parameter 1: processingDateStr
                COALESCE(a.region_code, 'UN') AS region_code

            FROM fin_core.dim_accounts a
            -- Join with previous day's balance
            LEFT JOIN fin_core.fact_daily_balances prev
                ON a.account_id = prev.account_id
                AND prev.balance_date = DATE_SUB(CAST(? AS DATE), 1) -- Parameter 2: processingDateStr
            -- Join with today's movements
            LEFT JOIN default.tmp_daily_movements_stg m
                ON a.account_id = m.account_id
            WHERE a.status IN ('OPEN', 'FROZEN', 'DORMANT');
            """;
    }

    /**
     * The main method to execute the daily balance processing workflow.
     */
    public void run() {
        String processingDateStr = config.getProperty("processing.date");
        if (processingDateStr == null || processingDateStr.isEmpty()) {
            logger.error("Processing date not configured. Please set 'processing.date' in config.properties.");
            return;
        }

        LocalDate processingDate = LocalDate.parse(processingDateStr);
        String previousDateStr = processingDate.minusDays(1).format(DateTimeFormatter.ISO_DATE);

        logger.info("Starting Daily Balance Processor for date: {}", processingDateStr);
        logger.info("Previous day for balance lookup: {}", previousDateStr);

        try (Connection conn = getConnection()) {
            // Hive JDBC often doesn't fully support transactions like traditional RDBMS.
            // Operations are often auto-committed. We'll set it explicitly, but be aware of Hive's limitations.
            // conn.setAutoCommit(false); // Can try, but Hive might ignore or have partial support

            // 1. Ensure target database exists
            executeDdl(conn, "CREATE DATABASE IF NOT EXISTS fin_core");

            // 2. Create the Daily Balance Snapshot Table if it doesn't exist
            executeDdl(conn, getCreateDailyBalancesTableSql());

            // 3. Temporary table to hold today's net movements per account
            // Drop existing temp table to ensure a clean state
            executeDdl(conn, "DROP TABLE IF EXISTS default.tmp_daily_movements_stg");

            // Create and populate the temporary table
            // The two '?' placeholders in the SQL both represent the processingDateStr
            executeDml(conn, getCreateTempDailyMovementsSql(), processingDateStr, processingDateStr);


            // 4. Calculate End of Day Balances and Insert Overwrite the partition
            // The '?' placeholders represent the processingDateStr
            executeDml(conn, getInsertDailyBalancesSql(), processingDateStr, processingDateStr);

            // Optional: If autoCommit is false, commit changes (unlikely to be fully effective in Hive)
            // conn.commit();
            logger.info("Daily balance processing completed successfully for date: {}", processingDateStr);

        } catch (SQLException e) {
            logger.error("Database error during daily balance processing for date {}. Rolling back any potential transactions.", processingDateStr, e);
            // Optional: If autoCommit is false and rollbacks are supported, attempt rollback
            // try (Connection conn = getConnection()) { if (conn != null && !conn.getAutoCommit()) conn.rollback(); } catch (SQLException ex) { logger.error("Error during rollback", ex); }
        } catch (Exception e) {
            logger.error("An unexpected error occurred during daily balance processing for date {}", processingDateStr, e);
        } finally {
            // 5. Clean up temporary table, even if an error occurred in main logic
            // This needs a new connection if the previous one failed, or should be handled carefully
            try (Connection cleanupConn = getConnection()) {
                executeDdl(cleanupConn, "DROP TABLE IF EXISTS default.tmp_daily_movements_stg");
            } catch (SQLException e) {
                logger.warn("Failed to drop temporary table during cleanup. It might have already been dropped or never created.", e);
            }
            logger.info("Finished Daily Balance Processor for date: {}", processingDateStr);
        }
    }

    public static void main(String[] args) {
        Properties config = new Properties();
        try (InputStream input = DailyBalanceProcessor.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                logger.error("Sorry, unable to find config.properties");
                return;
            }
            config.load(input);
        } catch (IOException ex) {
            logger.error("Error loading configuration properties", ex);
            return;
        }

        DailyBalanceProcessor processor = new DailyBalanceProcessor(config);
        processor.run();
    }
}
```

---

### 4. `src/main/resources/logback.xml` (Optional, for logging configuration)

```xml
<configuration>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <root level="info">
        <appender-ref ref="STDOUT" />
    </root>

    <!-- Suppress verbose Hive/Hadoop logs if needed -->
    <logger name="org.apache.hive" level="warn"/>
    <logger name="org.apache.hadoop" level="warn"/>
    <logger name="org.apache.http" level="warn"/>
    <logger name="io.netty" level="warn"/>
</configuration>
```

---

### How to Run:

1.  **Save the files:**
    *   `pom.xml` in the project root.
    *   `config.properties` in `src/main/resources/`.
    *   `DailyBalanceProcessor.java` in `src/main/java/com/example/processor/`.
    *   `logback.xml` in `src/main/resources/` (optional, but good practice).
2.  **Update `config.properties`:** Change `hive.jdbc.url`, `user`, `password` to match your HiveServer2 setup.
3.  **Compile and package (using Maven):**
    ```bash
    cd your-project-directory
    mvn clean package
    ```
    This will create a `hive-daily-balances-1.0-SNAPSHOT-jar-with-dependencies.jar` file in your `target/` directory.
4.  **Execute the JAR:**
    ```bash
    java -jar target/hive-daily-balances-1.0-SNAPSHOT-jar-with-dependencies.jar
    ```

### Key Changes and Considerations:

*   **SQL as Strings:** The HQL statements are now embedded as Java multi-line strings (text blocks in Java 15+). For older Java versions, you'd use string concatenation (`+`).
*   **JDBC Driver:** `org.apache.hive.jdbc.HiveDriver` is explicitly loaded.
*   **Parameterization:**
    *   Hardcoded dates like `'2024-01-01'` are replaced with `?` placeholders in the SQL.
    *   `PreparedStatement` is used to set these parameters, providing type safety and preventing SQL injection.
    *   The Java `LocalDate` API is used to calculate the processing date and the previous day's date dynamically.
*   **Error Handling:** `try-catch` blocks for `SQLException` and `IOException` (for config loading) are included.
*   **Resource Management:** `try-with-resources` ensures that JDBC `Connection`, `Statement`, and `PreparedStatement` objects are automatically closed, preventing resource leaks.
*   **Logging:** `slf4j` is used for logging, making the application's execution transparent.
*   **Temporary Table Lifecycle:** The temporary table (`tmp_daily_movements_stg`) is explicitly dropped before creation and after the main insert, mimicking the HQL behavior.
*   **Hive Transactionality:** Hive's support for transactions (especially for `INSERT OVERWRITE` into partitioned tables) can vary depending on the Hive version and configuration. While `TBLPROPERTIES ('transactional'='true')` is in your HQL, standard JDBC `commit()`/`rollback()` might not behave identically to a traditional RDBMS. The code reflects this by being more "batch-oriented" rather than "transactional" in the Java sense.
*   **SQL Syntax:** The SQL itself remains largely identical to your HQL, as Hive uses SQL-like syntax.
*   **External Configuration:** Database credentials and the processing date are read from `config.properties`, making the application more flexible without recompilation.