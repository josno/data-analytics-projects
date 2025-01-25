/* CREATE TABLES */

-- Create Database
CREATE DATABASE "D597 Task 1" WITH OWNER = postgres;

-- Create staging table

CREATE TABLE staging (
	region varchar(255),
    country varchar(255),
    item_type varchar(255),
    sales_channel varchar(255),
    order_priority char,
    order_date date,
    order_id integer,
    ship_date date,
    units_sold integer,
    unit_price numeric,
    unit_cost numeric,
    total_revenue numeric,
    total_cost numeric,
    total_profit numeric
);

-- Import data to staging table
COPY staging
FROM 'C:\Users\Public\Sales_Records.csv'
DELIMITER ','
CSV HEADER;

-- Create Model Tables
-- Location table
CREATE TABLE location (
	country varchar(255) PRIMARY KEY,
	region varchar(255)
);

-- Create enumerated type for sales channel
CREATE TYPE channel AS ENUM ('Online', 'Offline');

-- Order Detail Table
CREATE TABLE order_detail (
	order_id integer PRIMARY KEY,
	order_date date,
	ship_date date,
	sales_channel channel,
	order_priority char,
	country varchar(255) REFERENCES location(country)
);

-- Item Table
CREATE TABLE item (
	item_type varchar(255) PRIMARY KEY,
	unit_price numeric(6,2) NOT NULL,
	unit_cost numeric(6,2) NOT NULL
);

-- Order Table
CREATE TABLE "order" (
	order_id integer REFERENCES order_detail(order_id),
	item_type varchar(255) REFERENCES item(item_type),
	units_sold integer NOT NULL,
	PRIMARY KEY (order_id, item_type)
);



/* MAP AND INSERT DATA FROM STAGING TO TABLES */

-- Insert Data From Staging to Tables
-- location table
INSERT INTO location (country, region)
SELECT DISTINCT country, region
FROM staging;

-- Check inserted values
SELECT *
FROM "location";

-- order_detail
-- Alter table varchar to enumerated sales_channel in staging
ALTER TABLE staging
	ALTER COLUMN sales_channel TYPE channel USING sales_channel::channel; 

INSERT INTO order_detail (
	order_id, 
	order_date, 
	ship_date, 
	sales_channel,
	order_priority,
	country )
SELECT 
	order_id, 
	order_date, 
	ship_date, 
	sales_channel,
	order_priority,
	country
FROM staging;

-- Check inserted values
SELECT *
FROM order_detail;

-- Insert to item Table
INSERT INTO item (
	item_type,
	unit_price,
	unit_cost)
SELECT DISTINCT
	item_type,
	unit_price,
	unit_cost
FROM staging;

-- Check inserted values
SELECT *
FROM item;

-- Insert to order Table
INSERT INTO "order" (
	order_id,
	item_type,
	units_sold)
SELECT
	order_id,
	item_type,
	units_sold
FROM staging;

-- Check inserted values
SELECT *
FROM "order";

-- Drop temporary table after import
-- DROP TABLE staging;



