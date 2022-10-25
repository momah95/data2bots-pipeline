---product with highest review
SELECT SUM (review) AS total_reviews, dim_products.product_id, product_name
FROM somtmoma5254_staging.dim_products
JOIN somtmoma5254_staging.reviews ON dim_products.product_id = reviews.product_id
GROUP BY dim_products.product_id
ORDER BY SUM (review) DESC;

---most ordered date
SELECT MAX (quantity) AS quantity, order_date
FROM somtmoma5254_staging.dim_products
JOIN somtmoma5254_staging.orders ON dim_products.product_id = orders.product_id
WHERE dim_products.product_id = 22
GROUP BY order_date
ORDER BY MAX (quantity) DESC;

---was date public holiday? date:2022-04-02
---public holidays
SELECT calendar_dt FROM if_common.dim_dates
WHERE day_of_the_week_num BETWEEN 1 AND 5 
AND working_day = FALSE;

---total review points
SELECT SUM(review)
FROM somtmoma5254_staging.reviews
WHERE reviews.product_id = 22;

--- % distribution of reviews
--- 5 stars (23%)
SELECT COUNT(review)
FROM somtmoma5254_staging.reviews
WHERE review = 5 AND reviews.product_id = 22;
--- 4 stars (22%)
SELECT COUNT(review)
FROM somtmoma5254_staging.reviews
WHERE review = 4 AND reviews.product_id = 22;
--- 3 stars (18%)
SELECT COUNT(review)
FROM somtmoma5254_staging.reviews
WHERE review = 3 AND reviews.product_id = 22;
--- 2 stars (20%)
SELECT COUNT(review)
FROM somtmoma5254_staging.reviews
WHERE review = 2 AND reviews.product_id = 22;
--- 1 star (17%)
SELECT COUNT(review)
FROM somtmoma5254_staging.reviews
WHERE review = 1 AND reviews.product_id = 22;

--- % distribution of early and late shipments
---all shipments for product_id = 22 (136)
SELECT COUNT(shipment_date)
FROM somtmoma5254_staging.shipments_deliveries
JOIN somtmoma5254_staging.orders ON orders.order_id = shipments_deliveries.order_id
WHERE product_id = 22;

---late shipments for product_id = 22 (6)
CREATE TABLE late_product_shipment
AS
SELECT shipment_date,order_date,shipment_date - order_date  AS days_passed
FROM somtmoma5254_staging.shipments_deliveries
JOIN somtmoma5254_staging.orders ON orders.order_id = shipments_deliveries.order_id
WHERE product_id = 22 AND shipments_deliveries.delivery_date IS NULL;

SELECT COUNT(shipment_date) AS late_shipments
FROM late_product_shipment
WHERE days_passed >= 6;
