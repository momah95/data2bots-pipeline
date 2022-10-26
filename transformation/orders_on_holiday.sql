SELECT month_of_the_year_num AS month,
COUNT (DISTINCT order_id) AS orders FROM (
SELECT order_id,month_of_the_year_num  FROM somtmoma5254_staging.orders
JOIN(
SELECT * FROM if_common.dim_dates
WHERE day_of_the_week_num BETWEEN 1 AND 5 
AND working_day = FALSE
AND calendar_dt BETWEEN '2021-10-01' AND '2022-11-11') AS orders_on_holiday ON order_date = calendar_dt) AS public_orders
GROUP BY month_of_the_year_num ;


