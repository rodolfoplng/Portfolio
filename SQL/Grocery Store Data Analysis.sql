/*

Grocery Store Data Analysis

Source: Grocery Store Database found at https://github.com/rodolfoplng/Creating-Grocery-Store-Database-with-Docker-and-PostgreSQL

Skills: Subqueries, Joins, CTE's, Aggregate Functions, Group By Extensions (Cube, Rollup), Views

*Queried using PostgreSQL

*/


-- Listing the first and last sales in the database

SELECT *
FROM sales
WHERE salesdate IN (
    SELECT MIN(salesdate) FROM sales
    UNION ALL
    SELECT MAX(salesdate) FROM sales
);



-- Top 20 sold products (ranked per quantity)

SELECT 
    p.productid,
    p.productname,
    SUM(s.quantity) AS total_sold,
    RANK() OVER (ORDER BY SUM(s.quantity) DESC) AS rank_position
FROM sales s
INNER JOIN products p 
    ON s.productid = p.productid
GROUP BY p.productid, p.productname
ORDER BY rank_position
LIMIT 20;



-- Customers who purchased more than 10 times in January 2018, with their cities

SELECT 
    c.customerid,
    ci.cityname,
    COUNT(*) AS purchase_count
FROM sales s
INNER JOIN customers c 
    ON s.customerid = c.customerid
INNER JOIN cities ci
    ON c.cityid = ci.cityid
WHERE s.salesdate >= '2018-01-01'
  AND s.salesdate <  '2018-02-01'
GROUP BY c.customerid, ci.cityname
HAVING COUNT(*) > 10
ORDER BY purchase_count DESC, ci.cityname;



-- Finding the employee id, hire date, age and most sold category of the most senior employee in the company

WITH oldest AS (
    SELECT employeeid, hiredate, birthdate 
	FROM employees 
	WHERE hiredate = (SELECT MIN(hiredate) FROM employees)
),
category_sales AS (
    SELECT 
    e.employeeid,
    c.categoryname,
    COUNT(*) AS total_sold
	FROM employees e
	INNER JOIN sales s 
    	ON e.employeeid = s.salespersonid
	INNER JOIN products p 
   		ON s.productid = p.productid
	INNER JOIN categories c
    	ON p.categoryid = c.categoryid
	GROUP BY e.employeeid, c.categoryname
	ORDER BY e.employeeid, total_sold DESC
)
SELECT 
    o.employeeid,
    o.hiredate,
    o.birthdate,
    cs.categoryname,
    cs.total_sold
FROM oldest o
INNER JOIN category_sales cs ON o.employeeid = cs.employeeid
WHERE cs.total_sold = (
    SELECT MAX(total_sold)
    FROM category_sales
    WHERE employeeid = o.employeeid
);



-- Total revenue per city and month.

SELECT
  ci.cityname,
  EXTRACT(MONTH FROM s.salesdate) AS month,
  SUM(p.price * s.quantity) AS total_revenue
FROM sales s
INNER JOIN customers c ON c.customerid = s.customerid
INNER JOIN cities ci   ON ci.cityid     = c.cityid
INNER JOIN products p  ON p.productid   = s.productid
GROUP BY CUBE (ci.cityname, month)
ORDER BY ci.cityname NULLS LAST, month NULLS LAST;



-- Creating view for total monthly sales (revenue) per employee

CREATE VIEW sales_per_month_employee AS
SELECT
  e.employeeid,
  EXTRACT(YEAR  FROM s.salesdate)::int AS sales_year,
  EXTRACT(MONTH FROM s.salesdate)::int AS sales_month,
  SUM(p.price * s.quantity) AS total_sales_month
FROM employees e
INNER JOIN sales s   ON e.employeeid = s.salespersonid
INNER JOIN products p ON p.productid = s.productid
GROUP BY
  e.employeeid,
  sales_year,
  sales_month;
  
  
  
-- Using the sales_per_month_employee view to find the average monthly sales per employee in 2018

SELECT
  employeeid,
  ROUND(AVG(total_sales_month), 2) AS avg_sales_per_month_2018
FROM sales_per_month_employee
WHERE sales_year = 2018
GROUP BY employeeid
ORDER BY avg_sales_per_month_2018 DESC;



-- Total revenue per products and categories (with subtotals)

SELECT
    CASE WHEN GROUPING(c.categoryname) = 1 THEN 'ALL CATEGORIES' 
         ELSE c.categoryname END AS categoryname,
    CASE WHEN GROUPING(p.productname) = 1 THEN 'ALL PRODUCTS' 
         ELSE p.productname END AS productname,
    SUM(s.quantity * p.price) AS total_revenue
FROM categories c
INNER JOIN products p ON p.categoryid = c.categoryid
INNER JOIN sales s ON s.productid = p.productid
GROUP BY ROLLUP (c.categoryname, p.productname)



-- Finding the top 5 best-selling products (revenue) by city

WITH ranked AS (
  SELECT
    ci.cityname,
    p.productname,
    SUM(s.quantity * p.price) AS total_revenue,
    ROW_NUMBER() OVER (
      PARTITION BY ci.cityname
      ORDER BY SUM(s.quantity * p.price) DESC, p.productname
    ) AS rn
  FROM sales s
  INNER JOIN products p  ON s.productid = p.productid
  INNER JOIN customers c ON s.customerid = c.customerid
  INNER JOIN cities ci   ON ci.cityid    = c.cityid
  GROUP BY ci.cityname, p.productname
)
SELECT cityname, productname, total_revenue
FROM ranked
WHERE rn <= 5
ORDER BY cityname, rn;
