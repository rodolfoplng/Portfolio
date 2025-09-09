/*

Grocery Store ABC distribution

Source: Grocery Store Database found at https://github.com/rodolfoplng/Creating-Grocery-Store-Database-with-Docker-and-PostgreSQL

Skills: Views, Materialized Views, CTE's, User-Defined Functions

*Queried using PostgreSQL

*/


-- Creating view for total monthly sales (revenue) per product

CREATE VIEW sales_per_month_product AS
SELECT
	p.productid,
  	p.productname,
  	EXTRACT(YEAR  FROM s.salesdate)::int AS sales_year,
  	EXTRACT(MONTH FROM s.salesdate)::int AS sales_month,
  	SUM(p.price * s.quantity) AS total_revenue_month
FROM products p
INNER JOIN sales s   ON p.productid = s.productid
GROUP BY
  	p.productid,
  	sales_year,
  	sales_month;


	
-- Finding the ABC items
	
WITH q1 AS (
  SELECT
    productid,
    productname,
    SUM(total_revenue_month) AS total_revenue_q1
  FROM sales_per_month_product
  WHERE sales_year = 2018
    AND sales_month IN (1,2,3)
  GROUP BY productid, productname
),
acum AS (
  SELECT
    q1.*,
    SUM(total_revenue_q1) OVER () AS total_q1,
    SUM(total_revenue_q1) OVER (ORDER BY total_revenue_q1 DESC, productid) AS running_q1
  FROM q1
)
SELECT
  productid,
  productname,
  total_revenue_q1,
  running_q1,
  ROUND(running_q1 / NULLIF(total_q1,0), 4) AS accumulated_share,
  CASE
    WHEN running_q1 / NULLIF(total_q1,0) <= 0.80 THEN 'A'           -- top 80%
    WHEN running_q1 / NULLIF(total_q1,0) <= 0.95 THEN 'B'           -- next 15% (to 95%)
    ELSE 'C'                                                        -- remaining 5% (to 100%)
  END AS abc
FROM acum
ORDER BY total_revenue_q1 DESC, productid;



-- Materialized View for better query performance

CREATE MATERIALIZED VIEW mv_sales_per_month_product AS
SELECT	
	p.productid,
  	p.productname,
  	EXTRACT(YEAR  FROM s.salesdate)::int AS sales_year,
  	EXTRACT(MONTH FROM s.salesdate)::int AS sales_month,
  	SUM(p.price * s.quantity) AS total_revenue_month
FROM products p
INNER JOIN sales s ON p.productid = s.productid
GROUP BY
  	p.productid,
  	sales_year,
  	sales_month;



-- Creating function to find the ABC items for a given year, quarter and category thresholds 

CREATE OR REPLACE FUNCTION abc_product_yq(
  p_year    int,
  p_quarter int,
  p_a       numeric,  -- accumulative thresholds for "A" items (eg.: 0.80)
  p_b       numeric   -- accumulative thresholds for "B" items (eg.: 0.95)
)
RETURNS TABLE (
  productid int,
  productname text,
  sales_year int,
  sales_quarter int,
  total_revenue_q numeric,
  running_q numeric,
  accumulated_share numeric,
  abc text
)
LANGUAGE sql AS $$
WITH monthly AS (
  SELECT
    productid,
    productname,
    sales_year,
    ((sales_month - 1) / 3 + 1)::int AS sales_quarter,
    total_revenue_month
  FROM mv_sales_per_month_product
  WHERE sales_year = p_year
    AND ((sales_month - 1) / 3 + 1) = p_quarter
),
q_agg AS (
  SELECT
    productid, productname, sales_year, sales_quarter,
    SUM(total_revenue_month) AS total_revenue_q
  FROM monthly
  GROUP BY productid, productname, sales_year, sales_quarter
),
acum AS (
  SELECT
    q.*,
    SUM(total_revenue_q) OVER () AS total_q,
    SUM(total_revenue_q) OVER (ORDER BY total_revenue_q DESC, productid) AS running_q
  FROM q_agg q
)
SELECT
  productid, productname, sales_year, sales_quarter,
  total_revenue_q, running_q,
  ROUND(running_q / NULLIF(total_q, 0), 4) AS accumulated_share,
  CASE
    WHEN running_q / NULLIF(total_q, 0) <= p_a THEN 'A'
    WHEN running_q / NULLIF(total_q, 0) <= p_b THEN 'B'
    ELSE 'C'
  END AS abc
FROM acum
ORDER BY total_revenue_q DESC, productid;
$$;



-- Testing the function

SELECT * FROM abc_product_yq(2018, 1, 0.8, 0.95);



-- Returning the ABC distribution (counts and % of items) for the given quarter 

SELECT
  abc,
  COUNT(*) AS items,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_items,
  SUM(total_revenue_q) AS revenue_q,
  ROUND(100.0 * SUM(total_revenue_q) / SUM(SUM(total_revenue_q)) OVER (), 2) AS pct_revenue
FROM abc_product_yq(2018, 1, 0.8, 0.95)
GROUP BY abc
ORDER BY CASE abc WHEN 'A' THEN 1 WHEN 'B' THEN 2 ELSE 3 END;
