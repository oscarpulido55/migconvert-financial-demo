WITH
SalesIn2000 AS (
    -- Step 1: Filter sales data to include only transactions from the calendar year 2000.
    -- This pre-filters the data to reduce the amount processed in later steps.
    SELECT
        s.prod_id,
        s.amount_sold
    FROM
        sh.sales s
    JOIN
        sh.times t ON s.time_id = t.time_id
    WHERE
        t.calendar_year = 2000
),
ProductSalesAggregation AS (
    -- Step 2: Aggregate the sales data to get the total sales amount for each product.
    -- This summarizes the sales for each product that had sales in 2000.
    SELECT
        prod_id,
        SUM(amount_sold) AS total_sales_amount
    FROM
        SalesIn2000
    GROUP BY
        prod_id
),
RankedProducts AS (
    -- Step 3: Join aggregated sales with product details and rank products within each subcategory.
    -- DENSE_RANK() is used to handle ties; products with the same sales amount will receive the same rank.
    SELECT
        p.prod_name,
        p.prod_subcategory,
        psa.total_sales_amount,
        DENSE_RANK() OVER (PARTITION BY p.prod_subcategory ORDER BY psa.total_sales_amount DESC) as sales_rank
    FROM
        ProductSalesAggregation psa
    JOIN
        sh.products p ON psa.prod_id = p.prod_id
)
-- Final Selection: Select the top 3 ranked products from the final CTE.
SELECT
    rp.prod_subcategory,
    rp.prod_name,
    TO_CHAR(rp.total_sales_amount, 'FM$999,999,990.00') AS formatted_total_sales,
    rp.sales_rank
FROM
    RankedProducts rp
WHERE
    rp.sales_rank <= 3
ORDER BY
    rp.prod_subcategory,
    rp.sales_rank;
