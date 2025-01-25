-- Checks total rows and values are imported
SELECT *
FROM staging;

-- Checks that all order ids are unique 
SELECT 
	COUNT(DISTINCT order_id) = COUNT(*) as is_unique
FROM staging;

-- Check to see if item price and cost are the same per item type
SELECT 
	item_type,
	COUNT(DISTINCT unit_cost) as unit_cost,
	COUNT(DISTINCT unit_price) as unit_price
FROM staging
GROUP BY item_type;

-- List all unique entries for items by cost and price
SELECT 
	item_type,
	unit_cost,
	unit_price
FROM staging
GROUP BY 1,2,3;

/* Double check that every country is unique and has one-to-one match with region
If region count with more than 1 shows up, then we have nonuniqueness */
SELECT
	country,
	COUNT(DISTINCT region) as region_count
FROM staging
GROUP BY
	country
HAVING 
	COUNT(DISTINCT region) > 1; -- means that all countries matches with only one region

-- See all combinations of country and region which will become location table
SELECT 
	region,
	country
FROM staging
GROUP BY region, country
ORDER BY region;

-- Check that sales channel only have two values to be enumerated
SELECT DISTINCT sales_channel
FROM staging;

/* Nulls */
SELECT * 
FROM staging
WHERE 
	region IS NULL
	OR country IS NULL
	OR item_type IS NULL
	OR sales_channel IS NULL
	OR order_priority IS NULL
	OR order_date IS NULL
	OR order_id IS NULL
	OR ship_date IS NULL
	OR units_sold IS NULL
	OR unit_price IS NULL
	OR unit_cost IS NULL
	OR total_revenue IS NULL
	OR total_cost IS NULL
	OR total_profit IS NULL;

/* Do we have negatives numbers? */
SELECT * 
FROM staging
WHERE 
	units_sold < 0
	OR unit_price < 0
	OR unit_cost < 0
	OR total_revenue < 0
	OR total_cost < 0
	OR total_profit < 0;

/*Check min/max range of numeric attributes */
SELECT 
	MIN(order_date) AS min_order_date,
	MAX(order_date) AS max_order_date,
	MIN(ship_date) AS min_ship_date,
	MAX(ship_date) AS max_ship_date,
	MIN(units_sold) AS min_units_sold,
	MAX(units_sold) AS max_units_sold,
	MIN(unit_price) AS min_unit_price,
	MAX(unit_price) AS max_unit_price,
	MIN(unit_cost) AS min_unit_cost,
	MAX(unit_cost) AS max_unit_cost,
	MIN(total_revenue) AS min_total_revenue,
	MAX(total_revenue) AS max_total_revenue,
	MIN(total_profit) AS min_total_profit,
	MAX(total_profit) AS max_total_profit
FROM staging;

/* Are ship dates before order dates? */
SELECT *
FROM staging
WHERE
	ship_date < order_date;
	
/* Are any prices less than cost? */
SELECT *
FROM staging
WHERE
	unit_price < unit_cost;




