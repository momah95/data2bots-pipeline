import boto3
import psycopg2
import csv
import configparser
from botocore import UNSIGNED
from botocore.client import Config

#extract data from s3
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
bucket_name = "d2b-internal-assessment-bucket"
response = s3.list_objects(Bucket=bucket_name, Prefix="orders_data")
s3.list_objects_v2(Bucket = bucket_name)

s3.download_file(bucket_name, "orders_data/orders.csv", "orders.csv")
s3.download_file(bucket_name, "orders_data/reviews.csv", "reviews.csv")
s3.download_file(bucket_name, "orders_data/shipment_deliveries.csv", "shipment_deliveries.csv")

# initialize a connection to postgres database:
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("postgres_config", "database")
user = parser.get("postgres_config", "username")
password = parser.get("postgres_config", "password")
host = parser.get("postgres_config", "host")
port = parser.get("postgres_config", "port")

conn = psycopg2.connect(
    "dbname=" + dbname
    + " user=" + user
    + " password=" + password
    + " host=" + host,
    port = port)

if conn is None:
 print("Error connecting to the database")
else:
 print("Connection established!")

#create tables in postgresSQL
my_query = """DROP TABLE IF EXISTS somtmoma5254_staging.dim_customers;
CREATE  TABLE somtmoma5254_staging.dim_customers (LIKE if_common.dim_customers including all);
INSERT into somtmoma5254_staging.dim_customers
SELECT * 
FROM if_common.dim_customers;
ALTER table somtmoma5254_staging.dim_customers add PRIMARY KEY (customer_id);

DROP TABLE IF EXISTS somtmoma5254_staging.dim_dates;
CREATE  TABLE somtmoma5254_staging.dim_dates (LIKE if_common.dim_dates including all);
INSERT into somtmoma5254_staging.dim_dates
SELECT * 
FROM if_common.dim_dates;
ALTER table somtmoma5254_staging.dim_dates add PRIMARY KEY (calendar_dt);

DROP TABLE IF EXISTS somtmoma5254_staging.dim_addresses;
CREATE  TABLE somtmoma5254_staging.dim_addresses (LIKE if_common.dim_addresses including all);
INSERT into somtmoma5254_staging.dim_addresses
SELECT * 
FROM if_common.dim_addresses;
ALTER table somtmoma5254_staging.dim_addresses add PRIMARY KEY (postal_code);

DROP TABLE IF EXISTS somtmoma5254_staging.dim_products;
CREATE  TABLE somtmoma5254_staging.dim_products (LIKE if_common.dim_products including all);
INSERT into somtmoma5254_staging.dim_products
SELECT * 
FROM if_common.dim_products;
ALTER table somtmoma5254_staging.dim_products add PRIMARY KEY (product_id);

DROP TABLE IF EXISTS somtmoma5254_staging.orders;
CREATE TABLE somtmoma5254_staging.orders( 
     order_id int PRIMARY KEY NOT NULL,
     customer_id  int NOT NULL ,
     order_date date NOT NULL,
     product_id  int NOT NULL ,
     unit_price int NOT NULL,
     quantity int NOT NULL,
     amount int NOT NULL,
     FOREIGN KEY (product_id) REFERENCES somtmoma5254_staging.dim_products(product_id),
     FOREIGN KEY (customer_id) REFERENCES somtmoma5254_staging.dim_customers(customer_id)
    );

DROP TABLE IF EXISTS somtmoma5254_staging.reviews;
CREATE TABLE somtmoma5254_staging.reviews( 
    review int NOT NULL,
    product_id int NOT NULL,
    FOREIGN KEY (product_id) REFERENCES somtmoma5254_staging.dim_products(product_id)
);

DROP TABLE IF EXISTS somtmoma5254_staging.shipments_deliveries;
CREATE TABLE somtmoma5254_staging.shipments_deliveries( 
    shipment_id int PRIMARY KEY NOT NULL,
    order_id int NOT NULL ,
    shipment_date date  NULL,
    delivery_date date  NULL,
    FOREIGN KEY (order_id) REFERENCES somtmoma5254_staging.orders(order_id)
);"""

my_cursor = conn.cursor()
my_cursor.execute(my_query)
conn.commit()

#load csv files into database
csv_file_name = 'C:/Users/USER/Documents/Data2Bots/orders.csv'
my_query2 = "COPY somtmoma5254_staging.orders FROM STDIN DELIMITER ',' CSV HEADER"
my_cursor.copy_expert(my_query2, open(csv_file_name, "r"))

csv_file_name = 'C:/Users/USER/Documents/Data2Bots/reviews.csv'
my_query3 = "COPY somtmoma5254_staging.reviews FROM STDIN DELIMITER ',' CSV HEADER"
my_cursor.copy_expert(my_query3, open(csv_file_name, "r"))

csv_file_name = 'C:/Users/USER/Documents/Data2Bots/shipment_deliveries.csv'
my_query4 = "COPY somtmoma5254_staging.shipments_deliveries FROM STDIN DELIMITER ',' CSV HEADER"
my_cursor.copy_expert(my_query4, open(csv_file_name, "r"))

conn.commit()

#create and load tables into analytics schema
my_query5 = """DROP TABLE IF EXISTS somtmoma5254_analytics.agg_public_holiday;
CREATE TABLE somtmoma5254_analytics.agg_public_holiday(
        ingestion_date date PRIMARY KEY NOT NULL,
	    tt_order_hol_jan int NOT NULL,
		tt_order_hol_feb int NOT NULL,
		tt_order_hol_mar int NOT NULL,
		tt_order_hol_apr int NOT NULL,
		tt_order_hol_may int NOT NULL,
		tt_order_hol_jun int NOT NULL,
		tt_order_hol_jul int NOT NULL,
		tt_order_hol_aug int NOT NULL,
		tt_order_hol_sep int NOT NULL,
		tt_order_hol_oct int NOT NULL,
		tt_order_hol_nov int NOT NULL,
		tt_order_hol_dec int NOT NULL
);

DROP TABLE IF EXISTS somtmoma5254_analytics.agg_shipments;
CREATE TABLE somtmoma5254_analytics.agg_shipments(
        ingestion_date date PRIMARY KEY NOT NULL,
	    tt_late_shipments int NOT NULL,
		tt_undelivered_items int NOT NULL
);

DROP TABLE IF EXISTS somtmoma5254_analytics.best_performing_product;
CREATE TABLE somtmoma5254_analytics.best_performing_product(
        ingestion_date date PRIMARY KEY NOT NULL,
	    product_name varchar NOT NULL,
		most_ordered_day date NOT NULL,
		is_public_holiday bool NOT NULL,
		tt_review_points int NOT NULL,
		pct_one_star_review float NOT NULL,
		pct_two_star_review float NOT NULL,
		pct_three_star_review float NOT NULL,
		pct_four_star_review float NOT NULL,
		pct_five_star_review float NOT NULL,
		pct_early_shipments float NOT NULL,
		pct_late_shipments float NOT NULL	
);

INSERT INTO somtmoma5254_analytics.agg_public_holiday
SELECT (SELECT CURRENT_DATE) AS ingestion_date, 0,
 
(SELECT orders FROM(
SELECT month_of_the_year_num AS month,
COUNT (DISTINCT order_id) AS orders FROM (
SELECT order_id,month_of_the_year_num  FROM somtmoma5254_staging.orders
JOIN(
SELECT * FROM if_common.dim_dates
WHERE day_of_the_week_num BETWEEN 1 AND 5 
AND working_day = FALSE
AND calendar_dt BETWEEN '2021-10-01' AND '2022-11-11') AS orders_on_holiday ON order_date = calendar_dt) AS public_orders
GROUP BY month_of_the_year_num)AS feb WHERE month = 2) AS tt_order_hol_feb ,
 
(SELECT orders FROM(
SELECT month_of_the_year_num AS month,
COUNT (DISTINCT order_id) AS orders FROM (
SELECT order_id,month_of_the_year_num  FROM somtmoma5254_staging.orders
JOIN(
SELECT * FROM if_common.dim_dates
WHERE day_of_the_week_num BETWEEN 1 AND 5 
AND working_day = FALSE
AND calendar_dt BETWEEN '2021-10-01' AND '2022-11-11') AS orders_on_holiday ON order_date = calendar_dt) AS public_orders
GROUP BY month_of_the_year_num)AS mar WHERE month = 3) AS tt_order_hol_mar,
 
(SELECT orders FROM(
SELECT month_of_the_year_num AS month,
COUNT (DISTINCT order_id) AS orders FROM (
SELECT order_id,month_of_the_year_num  FROM somtmoma5254_staging.orders
JOIN(
SELECT * FROM if_common.dim_dates
WHERE day_of_the_week_num BETWEEN 1 AND 5 
AND working_day = FALSE
AND calendar_dt BETWEEN '2021-10-01' AND '2022-11-11') AS orders_on_holiday ON order_date = calendar_dt) AS public_orders
GROUP BY month_of_the_year_num)AS apr WHERE month = 4) AS tt_order_hol_apr,
0,0,
 
(SELECT orders FROM(
SELECT month_of_the_year_num AS month,
COUNT (DISTINCT order_id) AS orders FROM (
SELECT order_id,month_of_the_year_num  FROM somtmoma5254_staging.orders
JOIN(
SELECT * FROM if_common.dim_dates
WHERE day_of_the_week_num BETWEEN 1 AND 5 
AND working_day = FALSE
AND calendar_dt BETWEEN '2021-10-01' AND '2022-11-11') AS orders_on_holiday ON order_date = calendar_dt) AS public_orders
GROUP BY month_of_the_year_num)AS jul WHERE month = 7) AS tt_order_hol_jul,
 
(SELECT orders FROM(
SELECT month_of_the_year_num AS month,
COUNT (DISTINCT order_id) AS orders FROM (
SELECT order_id,month_of_the_year_num  FROM somtmoma5254_staging.orders
JOIN(
SELECT * FROM if_common.dim_dates
WHERE day_of_the_week_num BETWEEN 1 AND 5 
AND working_day = FALSE
AND calendar_dt BETWEEN '2021-10-01' AND '2022-11-11') AS orders_on_holiday ON order_date = calendar_dt) AS public_orders
GROUP BY month_of_the_year_num)AS aug WHERE month = 8) AS tt_order_hol_aug, 0,
 
(SELECT orders FROM(
SELECT month_of_the_year_num AS month,
COUNT (DISTINCT order_id) AS orders FROM (
SELECT order_id,month_of_the_year_num  FROM somtmoma5254_staging.orders
JOIN(
SELECT * FROM if_common.dim_dates
WHERE day_of_the_week_num BETWEEN 1 AND 5 
AND working_day = FALSE
AND calendar_dt BETWEEN '2021-10-01' AND '2022-11-11') AS orders_on_holiday ON order_date = calendar_dt) AS public_orders
GROUP BY month_of_the_year_num)AS oct WHERE month = 10) AS tt_order_hol_oct,
 
(SELECT orders FROM(
SELECT month_of_the_year_num AS month,
COUNT (DISTINCT order_id) AS orders FROM (
SELECT order_id,month_of_the_year_num  FROM somtmoma5254_staging.orders
JOIN(
SELECT * FROM if_common.dim_dates
WHERE day_of_the_week_num BETWEEN 1 AND 5 
AND working_day = FALSE
AND calendar_dt BETWEEN '2021-10-01' AND '2022-11-11') AS orders_on_holiday ON order_date = calendar_dt) AS public_orders
GROUP BY month_of_the_year_num)AS nov WHERE month = 11) AS tt_order_hol_nov,0

INSERT INTO somtmoma5254_analytics.agg_shipments
SELECT (SELECT CURRENT_DATE) AS ingestion_date,
 
(SELECT COUNT (DISTINCT shipment_date) AS late_shipments
FROM somtmoma5254_analytics.late_shipments
WHERE days_passed >= 6) AS tt_late_shipments,

(SELECT COUNT (DISTINCT order_id) AS undelivered_shipments
FROM somtmoma5254_analytics.undelivered_shipments
WHERE days_passed >= 15) AS tt_undelivered_items

INSERT INTO somtmoma5254_analytics.best_performing_product
SELECT (SELECT CURRENT_DATE) AS ingestion_date,

(SELECT product_name
FROM somtmoma5254_staging.dim_products
JOIN somtmoma5254_staging.reviews ON dim_products.product_id = reviews.product_id
GROUP BY dim_products.product_id
ORDER BY SUM (review) DESC
LIMIT 1) AS product_name,

(SELECT order_date 
FROM (
SELECT MAX (quantity) AS quantity, order_date
FROM somtmoma5254_staging.dim_products
JOIN somtmoma5254_staging.orders ON dim_products.product_id = orders.product_id
WHERE dim_products.product_id = 22
GROUP BY order_date
ORDER BY quantity DESC
LIMIT 1) AS order_date) AS most_ordered_day, 'FALSE',

(SELECT COUNT(review) AS total_review_points
FROM somtmoma5254_staging.reviews
WHERE reviews.product_id = 22) AS tt_review_points, 

(SELECT CAST(ROUND(f_stars.count * 100.0 /t_stars.count,2) AS FLOAT) AS percentage 
FROM(
SELECT COUNT(review)AS count
FROM somtmoma5254_staging.reviews
WHERE review = 1 AND reviews.product_id = 22
) AS f_stars CROSS JOIN
(SELECT COUNT(review) AS count
FROM somtmoma5254_staging.reviews
WHERE reviews.product_id = 22
) AS t_stars) pct_one_star_review,

(SELECT CAST(ROUND(f_stars.count * 100.0 /t_stars.count,2) AS FLOAT) AS percentage 
FROM(
SELECT COUNT(review)AS count
FROM somtmoma5254_staging.reviews
WHERE review = 2 AND reviews.product_id = 22
) AS f_stars CROSS JOIN
(SELECT COUNT(review) AS count
FROM somtmoma5254_staging.reviews
WHERE reviews.product_id = 22
) AS t_stars) pct_two_star_review, 

(SELECT CAST(ROUND(f_stars.count * 100.0 /t_stars.count,2) AS FLOAT) AS percentage 
FROM(
SELECT COUNT(review)AS count
FROM somtmoma5254_staging.reviews
WHERE review = 3 AND reviews.product_id = 22
) AS f_stars CROSS JOIN
(SELECT COUNT(review) AS count
FROM somtmoma5254_staging.reviews
WHERE reviews.product_id = 22
) AS t_stars) AS pct_three_star_review,

(SELECT CAST(ROUND(f_stars.count * 100.0 /t_stars.count,2) AS FLOAT) AS percentage 
FROM(
SELECT COUNT(review)AS count
FROM somtmoma5254_staging.reviews
WHERE review = 4 AND reviews.product_id = 22
) AS f_stars CROSS JOIN
(SELECT COUNT(review) AS count
FROM somtmoma5254_staging.reviews
WHERE reviews.product_id = 22
) AS t_stars) AS pct_four_star_review,

(SELECT CAST(ROUND(f_stars.count * 100.0 /t_stars.count,2) AS FLOAT) AS percentage 
FROM(
SELECT COUNT(review)AS count
FROM somtmoma5254_staging.reviews
WHERE review = 5 AND reviews.product_id = 22
) AS f_stars CROSS JOIN
(SELECT COUNT(review) AS count
FROM somtmoma5254_staging.reviews
WHERE reviews.product_id = 22
) AS t_stars) AS pct_five_star_review ,

(SELECT 100 - (SELECT CAST(ROUND(l_ship.count * 100.0 /t_ship.count,2) AS FLOAT) AS percentage_late 
FROM(
 SELECT COUNT(shipment_date) AS count FROM(
SELECT shipment_date,order_date,shipment_date - order_date  AS days_passed
FROM somtmoma5254_staging.shipments_deliveries
JOIN somtmoma5254_staging.orders ON orders.order_id = shipments_deliveries.order_id
WHERE product_id = 22 AND shipments_deliveries.delivery_date IS NULL) AS late WHERE days_passed >=6 ) AS l_ship CROSS JOIN
(SELECT COUNT(shipment_date) AS count
FROM somtmoma5254_staging.shipments_deliveries
JOIN somtmoma5254_staging.orders ON orders.order_id = shipments_deliveries.order_id
WHERE product_id = 22
) AS t_ship) AS percentage_early) AS pct_early_shipments,

(SELECT CAST(ROUND(l_ship.count * 100.0 /t_ship.count,2) AS FLOAT) AS percentage_late
FROM(
 SELECT COUNT(shipment_date) AS count FROM(
SELECT shipment_date,order_date,shipment_date - order_date  AS days_passed
FROM somtmoma5254_staging.shipments_deliveries
JOIN somtmoma5254_staging.orders ON orders.order_id = shipments_deliveries.order_id
WHERE product_id = 22 AND shipments_deliveries.delivery_date IS NULL) AS late WHERE days_passed >=6 ) AS l_ship CROSS JOIN
(SELECT COUNT(shipment_date) AS count
FROM somtmoma5254_staging.shipments_deliveries
JOIN somtmoma5254_staging.orders ON orders.order_id = shipments_deliveries.order_id
WHERE product_id = 22
) AS t_ship) AS pct_late_shipments
"""

my_cursor.execute(my_query5)
conn.commit()


