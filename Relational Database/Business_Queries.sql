-- What are the top three grossing item types sold based on revenue?
SELECT 
	o.item_type,
	SUM(o.units_sold) AS total_units_sold,
	ROUND(AVG(i.unit_price),2) AS unit_price,
	-- All items priced by item type category so this unit price is the same per item
	ROUND(SUM(o.units_sold * i.unit_price),2) AS total_revenue
FROM "order" AS o
LEFT JOIN item AS i
ON o.item_type = i.item_type
GROUP BY o.item_type 
ORDER BY total_revenue DESC
-- Show the top three highest first
LIMIT 3;

-- Which items sold the most (by units) per quarter?
WITH item_units_sold_by_quarter AS (
SELECT
	item_type, 
	-- EXTRACT function generates calendar quarter based on datetime (Q1, Q2, Q3, Q4)
	EXTRACT(quarter FROM order_date) AS calendar_quarter,
	-- Adds units of items sold by quarter
	SUM(o.units_sold) AS total_units_sold
FROM "order" AS o
LEFT JOIN order_detail AS od
ON o.order_id = od.order_id
GROUP BY item_type, calendar_quarter
ORDER BY calendar_quarter, total_units_sold DESC
),
ranked_units_sold AS (
SELECT 
	*,
	-- Ranks total units sold by highest to lowest (1 being highest), grouped by quarter
	DENSE_RANK() OVER (PARTITION BY calendar_quarter ORDER BY total_units_sold DESC) as total_units_sold_rank_by_quarter
FROM item_units_sold_by_quarter
)
-- Generate max total units sold by quarter
SELECT 
	item_type,
	calendar_quarter,
	total_units_sold
FROM 
	ranked_units_sold
WHERE 
	total_units_sold_rank_by_quarter = 1;




-- What is the total revenue per region?
WITH country_region_by_order_id AS (
	SELECT 
		l.region,
		od.order_id
	FROM order_detail AS od
	LEFT JOIN "location" AS l
	ON od.country = l.country
),
price_units_sold_by_region AS (
	SELECT 
	i.unit_price,
	o.units_sold,
	cr_id.region
	FROM 
		"order" AS o
	LEFT JOIN
		item AS i
	ON 
		o.item_type = i.item_type
	LEFT JOIN
		country_region_by_order_id as cr_id
	ON 
		o.order_id = cr_id.order_id
)
SELECT
	SUM(units_sold) AS total_units_sold,
	-- AVG unit price is the average price of all units regardless of item type
	ROUND(AVG(unit_price),2) AS avg_unit_price,
	ROUND(SUM(units_sold * unit_price),2) AS total_revenue,
	region
FROM price_units_sold_by_region
GROUP BY 
	region
ORDER BY 
	total_revenue DESC;
