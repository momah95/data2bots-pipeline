---product with highest review
SELECT product_name
FROM somtmoma5254_staging.dim_products
JOIN somtmoma5254_staging.reviews ON dim_products.product_id = reviews.product_id
GROUP BY dim_products.product_id
ORDER BY SUM (review) DESC
LIMIT 1;

---most ordered date
SELECT order_date 
FROM (
SELECT MAX (quantity) AS quantity, order_date
FROM somtmoma5254_staging.dim_products
JOIN somtmoma5254_staging.orders ON dim_products.product_id = orders.product_id
WHERE dim_products.product_id = 22
GROUP BY order_date
ORDER BY quantity DESC
LIMIT 1) AS order_date;


---was date public holiday? date:2022-07-20
SELECT calendar_dt AS date FROM if_common.dim_dates
WHERE day_of_the_week_num BETWEEN 1 AND 5 AND calendar_dt IN (SELECT order_date 
FROM (
SELECT MAX (quantity) AS quantity, order_date
FROM somtmoma5254_staging.dim_products
JOIN somtmoma5254_staging.orders ON dim_products.product_id = orders.product_id
WHERE dim_products.product_id = 22
GROUP BY order_date
ORDER BY quantity DESC
LIMIT 1) AS order_date)
AND working_day = FALSE;

---total review points
SELECT COUNT(review) AS total_review_points
FROM somtmoma5254_staging.reviews
WHERE reviews.product_id = 22; 

--- % distribution of reviews
--- 5 stars (23%)
SELECT CAST(ROUND(f_stars.count * 100.0 /t_stars.count,2) AS FLOAT) AS percentage 
FROM(
SELECT COUNT(review)AS count
FROM somtmoma5254_staging.reviews
WHERE review = 5 AND reviews.product_id = 22
) AS f_stars CROSS JOIN
(SELECT COUNT(review) AS count
FROM somtmoma5254_staging.reviews
WHERE reviews.product_id = 22
) AS t_stars;								

--- 4 stars (22%)
SELECT CAST(ROUND(f_stars.count * 100.0 /t_stars.count,2) AS FLOAT) AS percentage 
FROM(
SELECT COUNT(review)AS count
FROM somtmoma5254_staging.reviews
WHERE review = 4 AND reviews.product_id = 22
) AS f_stars CROSS JOIN
(SELECT COUNT(review) AS count
FROM somtmoma5254_staging.reviews
WHERE reviews.product_id = 22
) AS t_stars;
--- 3 stars (18%)
SELECT CAST(ROUND(f_stars.count * 100.0 /t_stars.count,2) AS FLOAT) AS percentage 
FROM(
SELECT COUNT(review)AS count
FROM somtmoma5254_staging.reviews
WHERE review = 3 AND reviews.product_id = 22
) AS f_stars CROSS JOIN
(SELECT COUNT(review) AS count
FROM somtmoma5254_staging.reviews
WHERE reviews.product_id = 22
) AS t_stars;
--- 2 stars (20%)
SELECT CAST(ROUND(f_stars.count * 100.0 /t_stars.count,2) AS FLOAT) AS percentage 
FROM(
SELECT COUNT(review)AS count
FROM somtmoma5254_staging.reviews
WHERE review = 2 AND reviews.product_id = 22
) AS f_stars CROSS JOIN
(SELECT COUNT(review) AS count
FROM somtmoma5254_staging.reviews
WHERE reviews.product_id = 22
) AS t_stars;
--- 1 star (17%)
SELECT CAST(ROUND(f_stars.count * 100.0 /t_stars.count,2) AS FLOAT) AS percentage 
FROM(
SELECT COUNT(review)AS count
FROM somtmoma5254_staging.reviews
WHERE review = 1 AND reviews.product_id = 22
) AS f_stars CROSS JOIN
(SELECT COUNT(review) AS count
FROM somtmoma5254_staging.reviews
WHERE reviews.product_id = 22
) AS t_stars;

--- % distribution of early(95.6%) and late shipments(4.4%)
---all shipments for product_id = 22 (136)
--- % late shipments for product_id 
SELECT CAST(ROUND(l_ship.count * 100.0 /t_ship.count,2) AS FLOAT) AS percentage_late
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
) AS t_ship;

---%early shipments
SELECT 100 - (SELECT CAST(ROUND(l_ship.count * 100.0 /t_ship.count,2) AS FLOAT) AS percentage_late 
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
) AS t_ship) AS percentage_early;









