Converting a PySpark/Postgres solution to C#/BigQuery involves a fundamental shift in paradigm: from a DataFrame API-centric, distributed processing framework to a client-side C# application that orchestrates SQL queries against a serverless, columnar database like BigQuery.

Here's the C# equivalent, broken down into parts:

**Key Changes and Considerations:**

1.  **SparkSession -> BigQueryClient:** The `SparkSession` is replaced by `Google.Cloud.BigQuery.V2.BigQueryClient`.
2.  **DataFrame Operations -> SQL Queries (CTEs):** PySpark DataFrame transformations (`filter`, `groupBy`, `withColumn`, `join`, `agg`, `window`) are translated into BigQuery SQL queries, primarily using Common Table Expressions (CTEs) for readability and modularity.
3.  **HDFS Paths -> BigQuery Tables:** `hdfs://...` paths are replaced by fully qualified BigQuery table references (`project.dataset.table`).
4.  **Date/Time Handling:** Python `datetime` and `pytz` are replaced by C# `DateTime`, `DateTimeOffset`, and `TimeZoneInfo`, as well as BigQuery's rich `TIMESTAMP` functions.
5.  **`monotonically_increasing_id()`:** BigQuery does not have a direct equivalent for this *across the entire dataset in a deterministic, ordered fashion* like Spark does. For unique row identifiers within a query context, `GENERATE_UUID()` is used, or in some cases, `ROW_NUMBER()` within specific partitions. In this case, `GENERATE_UUID()` is suitable for `order_pk` to join back to quotes.
6.  **`F.expr("...")` and `F.when(...)`:** These map directly to SQL syntax (`CASE WHEN ... THEN ... END`, `INTERVAL`, `TIMESTAMP_SUB`, etc.).
7.  **`repartition()` and `sortWithinPartitions()`:** These are Spark-specific optimization hints. BigQuery handles query optimization internally; these do not have direct SQL equivalents that you explicitly write in the query.
8.  **Output:** Instead of writing Parquet to HDFS, the results are written to a new BigQuery table.
9.  **Error Handling:** C# uses `try-catch` for exceptions, and `Environment.Exit` for application termination.

---

### **1. C# Project Setup**

Create a new C# Console Application project.

Install the necessary NuGet package:

```bash
dotnet add package Google.Cloud.BigQuery.V2
dotnet add package NodaTime.TimeZones # For better timezone support, or stick to System.TimeZoneInfo
```

**Authentication:**
For `Google.Cloud.BigQuery.V2` to work, you need to set up authentication. The most common way is to set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the path of your service account key JSON file.

```bash
# Example for Linux/macOS
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/keyfile.json"

# Example for Windows PowerShell
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\path\to\your\keyfile.json"
```

---

### **2. C# Code (`AlgorithmicTradingPerformance.cs`)**

```csharp
using Google.Cloud.BigQuery.V2;
using System;
using System.Text;
using System.Globalization;
using System.Collections.Generic;
using System.Linq;

public class AlgorithmicTradingPerformance
{
    private readonly BigQueryClient _bigQueryClient;
    private readonly string _projectId; // Your Google Cloud Project ID
    private readonly string _datasetId; // Your BigQuery Dataset ID
    private readonly string _outputTableId = "trading_performance_analysis"; // Output table name

    // BigQuery Table Names
    private const string TradingEventsBaseTable = "trading_events_base";
    private const string ParentOrdersTable = "parent_orders";
    private const string Level1QuotesTable = "level1_quotes";
    private const string MarketTradesTable = "market_trades";

    public AlgorithmicTradingPerformance(BigQueryClient bigQueryClient, string projectId, string datasetId)
    {
        _bigQueryClient = bigQueryClient ?? throw new ArgumentNullException(nameof(bigQueryClient));
        _projectId = projectId ?? throw new ArgumentNullException(nameof(projectId));
        _datasetId = datasetId ?? throw new ArgumentNullException(nameof(datasetId));
    }

    /// <summary>
    /// Converts a local time (hour, minute) on a given date string to a UTC timestamp string.
    /// </summary>
    /// <param name="hour">Hour of the day (0-23).</param>
    /// <param name="minute">Minute of the hour (0-59).</param>
    /// <param name="dateStr">Date string in YYYY-MM-DD format.</param>
    /// <param name="tzId">Time zone ID, e.g., "America/New_York".</param>
    /// <returns>UTC timestamp string in YYYY-MM-DD HH:mm:ss format.</returns>
    public string LocalToUtcTime(int hour, int minute, string dateStr, string tzId = "America/New_York")
    {
        // Using NodaTime.TimeZones for better cross-platform timezone handling
        // Fallback to System.TimeZoneInfo if NodaTime is not used.
        try
        {
            var zone = NodaTime.DateTimeZoneProviders.Tzdb.GetZoneOrNull(tzId);
            if (zone == null)
            {
                // Fallback for systems where TZDB might not be fully configured or NodaTime isn't desired.
                // Note: System.TimeZoneInfo uses Windows/system-specific IDs (e.g., "Eastern Standard Time" for EST)
                // A mapping might be needed or different TZID needs to be passed
                Console.WriteLine($"Warning: TimeZoneId '{tzId}' not found via NodaTime. Falling back to System.TimeZoneInfo potentially.");
                return ConvertWithSystemTimeZoneInfo(hour, minute, dateStr, "Eastern Standard Time"); // Example fallback
            }

            var date = NodaTime.LocalDate.Parse(dateStr, CultureInfo.InvariantCulture);
            var localTime = date.At(new NodaTime.LocalTime(hour, minute));
            var zonedTime = zone.AtLeniently(localTime);
            return zonedTime.ToDateTimeUtc().ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error converting time using NodaTime, attempting System.TimeZoneInfo: {ex.Message}");
            // System.TimeZoneInfo has different IDs (e.g., "Eastern Standard Time")
            // This would require a mapping or specific ID to be passed.
            // For "America/New_York", on Windows, it's often "Eastern Standard Time"
            return ConvertWithSystemTimeZoneInfo(hour, minute, dateStr, "Eastern Standard Time");
        }
    }

    private string ConvertWithSystemTimeZoneInfo(int hour, int minute, string dateStr, string windowsTzId)
    {
        DateTime dateObj = DateTime.ParseExact(dateStr, "yyyy-MM-dd", CultureInfo.InvariantCulture);
        DateTime localDateTime = new DateTime(dateObj.Year, dateObj.Month, dateObj.Day, hour, minute, 0);

        TimeZoneInfo localTimeZone;
        try
        {
            // For Unix/macOS, use 'America/New_York'. For Windows, use 'Eastern Standard Time'.
            localTimeZone = OperatingSystem.IsWindows() 
                ? TimeZoneInfo.FindSystemTimeZoneById(windowsTzId) 
                : TimeZoneInfo.FindSystemTimeZoneById("America/New_York");
        }
        catch (TimeZoneNotFoundException)
        {
            Console.WriteLine($"Error: TimeZone '{windowsTzId}' or 'America/New_York' not found on this system.");
            throw; // Re-throw or handle appropriately
        }

        DateTime utcDateTime = TimeZoneInfo.ConvertTimeToUtc(localDateTime, localTimeZone);
        return utcDateTime.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
    }


    /// <summary>
    /// Executes the algorithmic trading performance analysis pipeline.
    /// </summary>
    /// <param name="runDate">The date for which to run the analysis, format YYYY-MM-DD.</param>
    public void ExecutePipeline(string runDate)
    {
        Console.WriteLine($"Starting algorithmic trading performance pipeline for date: {runDate}");

        // Fully qualified table names
        string tradingEventsBaseFQN = $"{_projectId}.{_datasetId}.{TradingEventsBaseTable}";
        string parentOrdersFQN = $"{_projectId}.{_datasetId}.{ParentOrdersTable}";
        string level1QuotesFQN = $"{_projectId}.{_datasetId}.{Level1QuotesTable}";
        string marketTradesFQN = $"{_projectId}.{_datasetId}.{MarketTradesTable}";
        string outputTableFQN = $"{_projectId}.{_datasetId}.{_outputTableId}_{runDate.Replace("-", "")}"; // Suffix with date for unique output

        // Calculate UTC market open/close times
        string utcTimeMarketOpen = LocalToUtcTime(9, 30, runDate);
        string utcTimeMarketClose = LocalToUtcTime(16, 0, runDate);

        Console.WriteLine($"UTC Market Open: {utcTimeMarketOpen}");
        Console.WriteLine($"UTC Market Close: {utcTimeMarketClose}");

        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.AppendLine($"WITH");

        // 1. Separate Event Stream into Fills
        queryBuilder.AppendLine($@"
        fills_df AS (
            SELECT *
            FROM `{tradingEventsBaseFQN}`
            WHERE
                (protocol_version = 'V1' AND status IN ('FILLED', 'PARTIAL') AND event_type = 'TRADE')
                OR (protocol_version >= 'V2' AND event_type = 'FILL')
        ),");

        // Get the closing fill price per order
        queryBuilder.AppendLine($@"
        close_fill_price_df AS (
            SELECT
                order_id AS close_order_id,
                client_id AS close_client_id,
                last_exec_price AS closing_price,
                last_exec_qty AS closing_qty
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY order_id, client_id ORDER BY event_timestamp) as rn
                FROM fills_df
            )
            WHERE rn = 1
        ),");

        // Aggregate fills per order
        queryBuilder.AppendLine($@"
        fills_agg_df AS (
            SELECT
                order_id,
                client_id,
                trade_date,
                LEAST(MIN(event_timestamp), MIN(routing_timestamp)) AS fill_FillStartTime,
                MIN(event_timestamp) AS fill_FirstFillTime,
                SUM(last_exec_qty) AS fill_TotalSharesExecuted,
                COUNT(last_exec_qty) AS fill_NumberOfFills,
                SAFE_DIVIDE(SUM(last_exec_qty * last_exec_price), SUM(last_exec_qty)) AS fill_AverageExecutionPrice,
                SUM(last_exec_qty * last_exec_price) AS fill_TotalMarketValueExecuted
            FROM fills_df
            GROUP BY 1, 2, 3
        ),");
            
        // 3. Execution Acknowledgements
        queryBuilder.AppendLine($@"
        acks_agg_df AS (
            SELECT
                order_id,
                client_id,
                trade_date,
                LEAST(MIN(event_timestamp), MIN(routing_timestamp)) AS ack_AckStartTime
            FROM `{tradingEventsBaseFQN}`
            WHERE status IN ('NEW', 'REPLACED') AND event_type = 'ACK'
            GROUP BY 1, 2, 3
        ),");

        // 4. Execution Terminations
        queryBuilder.AppendLine($@"
        terminations_agg_df AS (
            SELECT
                order_id,
                client_id,
                trade_date,
                GREATEST(MAX(event_timestamp), MAX(routing_timestamp)) AS term_ExecutionEndTime
            FROM `{tradingEventsBaseFQN}`
            WHERE status IN ('CANCELED', 'DONE_FOR_DAY', 'EXPIRED', 'REJECTED')
            GROUP BY 1, 2, 3
        ),");

        // 5. Bring it back to Parent Orders (enriched_orders_df)
        queryBuilder.AppendLine($@"
        enriched_orders_intermediate AS (
            SELECT
                p.*,
                f.fill_FillStartTime,
                f.fill_FirstFillTime,
                f.fill_TotalSharesExecuted,
                f.fill_NumberOfFills,
                f.fill_AverageExecutionPrice,
                f.fill_TotalMarketValueExecuted,
                a.ack_AckStartTime,
                t.term_ExecutionEndTime,
                c.closing_price,
                c.closing_qty
            FROM `{parentOrdersFQN}` AS p
            LEFT JOIN fills_agg_df AS f
                ON p.order_id = f.order_id AND p.client_id = f.client_id
            LEFT JOIN acks_agg_df AS a
                ON p.order_id = a.order_id AND p.client_id = a.client_id
            LEFT JOIN terminations_agg_df AS t
                ON p.order_id = t.order_id AND p.client_id = t.client_id
            LEFT JOIN close_fill_price_df AS c
                ON p.order_id = c.close_order_id AND p.client_id = c.close_client_id
        ),
        enriched_orders_df AS (
            SELECT
                *,
                GENERATE_UUID() AS order_pk, -- Added for unique row identification later
                LEAST(GREATEST(ack_AckStartTime, TIMESTAMP(@utc_market_open_time)), fill_FillStartTime) AS EffectiveStartTime,
                LEAST(term_ExecutionEndTime, TIMESTAMP(@utc_market_close_time)) AS EffectiveEndTime
            FROM enriched_orders_intermediate
        ),");

        // 6. Market Data Tick Metrics (complex time-based joins - nearest quotes)
        // This maps the find_nearest_quote logic into CTEs
        queryBuilder.AppendLine($@"
        start_quotes_cte AS (
            SELECT
                e.order_pk,
                q.best_bid AS start_best_bid,
                q.best_ask AS start_best_ask,
                q.quote_timestamp AS start_quote_timestamp
            FROM enriched_orders_df AS e
            JOIN `{level1QuotesFQN}` AS q
                ON e.ticker = q.ticker
               AND q.quote_timestamp BETWEEN TIMESTAMP_SUB(e.EffectiveStartTime, INTERVAL 10 MINUTE) AND e.EffectiveStartTime
            QUALIFY ROW_NUMBER() OVER (PARTITION BY e.order_pk ORDER BY ABS(UNIX_MICROS(e.EffectiveStartTime) - UNIX_MICROS(q.quote_timestamp))) = 1
        ),
        end_quotes_cte AS (
            SELECT
                e.order_pk,
                q.best_bid AS end_best_bid,
                q.best_ask AS end_best_ask
            FROM enriched_orders_df AS e
            JOIN `{level1QuotesFQN}` AS q
                ON e.ticker = q.ticker
               AND q.quote_timestamp BETWEEN TIMESTAMP_SUB(e.EffectiveEndTime, INTERVAL 10 MINUTE) AND e.EffectiveEndTime
            QUALIFY ROW_NUMBER() OVER (PARTITION BY e.order_pk ORDER BY ABS(UNIX_MICROS(e.EffectiveEndTime) - UNIX_MICROS(q.quote_timestamp))) = 1
        ),
        end_1m_quotes_cte AS (
            SELECT
                e.order_pk,
                q.best_bid AS end_plus1_best_bid,
                q.best_ask AS end_plus1_best_ask
            FROM enriched_orders_df AS e
            JOIN `{level1QuotesFQN}` AS q
                ON e.ticker = q.ticker
               AND q.quote_timestamp BETWEEN TIMESTAMP_SUB(TIMESTAMP_ADD(e.EffectiveEndTime, INTERVAL 1 MINUTE), INTERVAL 10 MINUTE) AND TIMESTAMP_ADD(e.EffectiveEndTime, INTERVAL 1 MINUTE)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY e.order_pk ORDER BY ABS(UNIX_MICROS(TIMESTAMP_ADD(e.EffectiveEndTime, INTERVAL 1 MINUTE)) - UNIX_MICROS(q.quote_timestamp))) = 1
        ),");

        // 7. Core VWAP during order existence
        queryBuilder.AppendLine($@"
        vwap_df AS (
            SELECT
                e.order_id,
                e.client_id,
                SUM(t.trade_size) AS market_interval_volume,
                SAFE_DIVIDE(SUM(t.trade_price * t.trade_size), SUM(t.trade_size)) AS market_interval_vwap
            FROM enriched_orders_df AS e
            INNER JOIN `{marketTradesFQN}` AS t
                ON e.ticker = t.ticker
               AND t.trade_timestamp >= e.EffectiveStartTime
               AND t.trade_timestamp <= e.EffectiveEndTime
            GROUP BY e.order_id, e.client_id
        )
        ");

        // Final Join and Calculations (similar to final_df in PySpark)
        queryBuilder.AppendLine($@"
        SELECT
            e.* EXCEPT (order_pk, Start_lower, End_lower, End_plus1, End_plus1_lower, End_plus5, End_plus5_lower), -- Exclude internal Pk and temp window bounds from final output
            vw.market_interval_volume,
            vw.market_interval_vwap,
            sq.start_best_bid,
            sq.start_best_ask,
            sq.start_quote_timestamp,
            eq.end_best_bid,
            eq.end_best_ask,
            eq1m.end_plus1_best_bid,
            eq1m.end_plus1_best_ask,
            -- Derived calculations
            SAFE_DIVIDE((sq.start_best_bid + sq.start_best_ask), 2) AS arrival_mid_price,
            CASE e.side
                WHEN 'BUY' THEN SAFE_DIVIDE((e.fill_AverageExecutionPrice - vw.market_interval_vwap), vw.market_interval_vwap) * 10000
                WHEN 'SELL' THEN SAFE_DIVIDE((vw.market_interval_vwap - e.fill_AverageExecutionPrice), vw.market_interval_vwap) * 10000
                ELSE NULL
            END AS slippage_from_vwap_bps,
            CASE e.side
                WHEN 'BUY' THEN (SAFE_DIVIDE((sq.start_best_bid + sq.start_best_ask), 2) - e.fill_AverageExecutionPrice) * e.fill_TotalSharesExecuted
                WHEN 'SELL' THEN (e.fill_AverageExecutionPrice - SAFE_DIVIDE((sq.start_best_bid + sq.start_best_ask), 2)) * e.fill_TotalSharesExecuted
                ELSE NULL
            END AS implementation_shortfall_pl,
            CASE e.side
                WHEN 'BUY' THEN (SAFE_DIVIDE((eq1m.end_plus1_best_bid + eq1m.end_plus1_best_ask), 2) - SAFE_DIVIDE((eq.end_best_bid + eq.end_best_ask), 2)) * e.fill_TotalSharesExecuted
                WHEN 'SELL' THEN (SAFE_DIVIDE((eq.end_best_bid + eq.end_best_ask), 2) - SAFE_DIVIDE((eq1m.end_plus1_best_bid + eq1m.end_plus1_best_ask), 2)) * e.fill_TotalSharesExecuted
                ELSE NULL
            END AS post_trade_1m_momentum,
            CASE e.side
                WHEN 'BUY' THEN (e.requested_shares - e.fill_TotalSharesExecuted) * (e.fill_AverageExecutionPrice - e.closing_price)
                WHEN 'SELL' THEN (e.requested_shares - e.fill_TotalSharesExecuted) * (e.closing_price - e.fill_AverageExecutionPrice)
                ELSE NULL
            END AS opportunity_cost_pl
        FROM enriched_orders_df AS e
        LEFT JOIN vwap_df AS vw ON e.order_id = vw.order_id AND e.client_id = vw.client_id
        LEFT JOIN start_quotes_cte AS sq ON e.order_pk = sq.order_pk
        LEFT JOIN end_quotes_cte AS eq ON e.order_pk = eq.order_pk
        LEFT JOIN end_1m_quotes_cte AS eq1m ON e.order_pk = eq1m.order_pk
        ");
        
        string sqlQuery = queryBuilder.ToString();

        // Prepare BigQuery parameters
        var parameters = new List<BigQueryParameter>
        {
            new BigQueryParameter("utc_market_open_time", BigQueryDbType.Timestamp, DateTime.ParseExact(utcTimeMarketOpen, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal)),
            new BigQueryParameter("utc_market_close_time", BigQueryDbType.Timestamp, DateTime.ParseExact(utcTimeMarketClose, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal))
        };
        
        // Configure the query job to write results to a new table
        var queryJobConfig = new QueryJobConfiguration(sqlQuery, parameters)
        {
            DestinationTable = _bigQueryClient.GetTableReference(outputTableFQN),
            WriteDisposition = WriteDisposition.WriteTruncate, // Overwrite the table if it exists
            CreateDisposition = CreateDisposition.CreateTableIfNeeded // Create table if it doesn't exist
        };

        try
        {
            Console.WriteLine("Submitting BigQuery job...");
            BigQueryJob job = _bigQueryClient.CreateQueryJob(queryJobConfig, JobFields.Status);
            job.PollUntilCompleted();

            if (job.Status.ErrorResult != null)
            {
                Console.WriteLine($"BigQuery job failed: {job.Status.ErrorResult.Message}");
                if (job.Status.Errors != null)
                {
                    foreach (var error in job.Status.Errors)
                    {
                        Console.WriteLine($"- {error.Message} (Reason: {error.Reason})");
                    }
                }
                throw new Exception("BigQuery job failed.");
            }

            Console.WriteLine($"Performance analysis complete. Results written to BigQuery table: {outputTableFQN}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred during BigQuery operation: {ex.Message}");
            throw; // Re-throw for further error handling upstream if desired
        }
    }
}
```

---

### **3. C# Entry Point (`Program.cs`)**

```csharp
using Google.Cloud.BigQuery.V2;
using System;
using System.Linq;

public class Program
{
    private const string ProjectId = "your-gcp-project-id"; // IMPORTANT: Replace with your GCP project ID
    private const string DatasetId = "your_bigquery_dataset_id"; // IMPORTANT: Replace with your BigQuery dataset ID

    public static void Main(string[] args)
    {
        if (args.Length < 1)
        {
            Console.WriteLine("Usage: dotnet run <run_date_YYYY-MM-DD>");
            Environment.Exit(1);
        }

        string runDate = args[0];

        try
        {
            // Initialize BigQueryClient (authentication is typically handled via GOOGLE_APPLICATION_CREDENTIALS)
            BigQueryClient client = BigQueryClient.Create(ProjectId);
            
            AlgorithmicTradingPerformance analyzer = new AlgorithmicTradingPerformance(client, ProjectId, DatasetId);
            analyzer.ExecutePipeline(runDate);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Fatal error: {ex.Message}");
            Console.WriteLine(ex.StackTrace);
            Environment.Exit(1);
        }
    }
}
```

---

### **To Run This Code:**

1.  **Replace Placeholders:** Update `ProjectId` and `DatasetId` in `Program.cs` with your actual Google Cloud Project ID and BigQuery Dataset ID.
2.  **Configure Authentication:** Ensure your `GOOGLE_APPLICATION_CREDENTIALS` environment variable is set up correctly, pointing to a service account key with sufficient permissions (BigQuery Data Editor, BigQuery Job User).
3.  **Create Tables in BigQuery:** Make sure you have the source tables (`trading_events_base`, `parent_orders`, `level1_quotes`, `market_trades`) in your specified dataset in BigQuery. The schema of these tables must match the columns used in the SQL query.
    *   `event_timestamp` and `routing_timestamp` should be `TIMESTAMP`.
    *   `trade_date` likely `DATE` or `STRING` in `YYYY-MM-DD`.
    *   Prices, quantities, sizes are likely `NUMERIC` or `FLOAT64`.
    *   `protocol_version`, `status`, `event_type`, `order_id`, `client_id`, `ticker`, `side` should be `STRING`.
    *   `requested_shares` should be `INT64` or `NUMERIC`.
4.  **Install NodaTime (Optional but Recommended for Time Zones):**
    `dotnet add package NodaTime`
    If you choose not to use NodaTime, the `LocalToUtcTime` method will fall back to `System.TimeZoneInfo`. Be aware that `System.TimeZoneInfo` uses different IDs for time zones (e.g., "Eastern Standard Time" on Windows instead of "America/New_York"). You might need to adjust the `windowsTzId` argument.
5.  **Compile and Run:**
    ```bash
    dotnet build
    dotnet run -- 2023-10-26 # Example run date
    ```

This C# application will connect to BigQuery, execute the generated SQL query (as a single job that leverages BigQuery's parallelism), and write the results to a new BigQuery table.