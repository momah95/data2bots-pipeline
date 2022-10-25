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

# initialize a connection postgres database:
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
my_query = """CREATE  TABLE somtmoma5254_staging.dim_customers (LIKE if_common.dim_customers including all);
INSERT into somtmoma5254_staging.dim_customers
SELECT * 
FROM if_common.dim_customers;
ALTER table somtmoma5254_staging.dim_customers add PRIMARY KEY (customer_id);


CREATE  TABLE somtmoma5254_staging.dim_dates (LIKE if_common.dim_dates including all);
INSERT into somtmoma5254_staging.dim_dates
SELECT * 
FROM if_common.dim_dates;
ALTER table somtmoma5254_staging.dim_dates add PRIMARY KEY (calendar_dt);


CREATE  TABLE somtmoma5254_staging.dim_addresses (LIKE if_common.dim_addresses including all);
INSERT into somtmoma5254_staging.dim_addresses
SELECT * 
FROM if_common.dim_addresses;
ALTER table somtmoma5254_staging.dim_addresses add PRIMARY KEY (postal_code);


CREATE  TABLE somtmoma5254_staging.dim_products (LIKE if_common.dim_products including all);
INSERT into somtmoma5254_staging.dim_products
SELECT * 
FROM if_common.dim_products;
ALTER table somtmoma5254_staging.dim_products add PRIMARY KEY (product_id);


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
CREATE TABLE somtmoma5254_staging.reviews( 
    review int NOT NULL,
    product_id int NOT NULL,
    FOREIGN KEY (product_id) REFERENCES somtmoma5254_staging.dim_products(product_id)
);
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
my_query5 = """CREATE TABLE somtmoma5254_analytics.agg_public_holiday(
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

CREATE TABLE somtmoma5254_analytics.agg_shipments(
        ingestion_date date PRIMARY KEY NOT NULL,
	    tt_late_shipments int NOT NULL,
		tt_undelivered_items int NOT NULL
);

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

INSERT INTO somtmoma5254_analytics.agg_public_holiday VALUES ('2022-10-18',0,17,13,12,0,0,13,17,0,16,12,0);
INSERT INTO somtmoma5254_analytics.agg_shipments VALUES ('2022-10-21',152,6586);
INSERT INTO somtmoma5254_analytics.best_performing_product VALUES ('2022-10-24','Scooter','2022-04-02',FALSE,967,23,22,18,20,17,95.6,4.4);
"""

my_cursor.execute(my_query5)
conn.commit()


