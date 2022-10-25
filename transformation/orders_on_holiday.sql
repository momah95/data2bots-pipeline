CREATE TABLE public_holiday_last_year
AS
SELECT * FROM if_common.dim_dates
WHERE day_of_the_week_num BETWEEN 1 AND 5 
AND working_day = FALSE
AND calendar_dt BETWEEN '2021-10-01' AND '2022-11-11';

CREATE TABLE orders_on_public_holiday
AS
SELECT order_id,month_of_the_year_num  FROM somtmoma5254_staging.orders
JOIN public_holiday_last_year ON order_date = calendar_dt;

SELECT month_of_the_year_num AS month,
COUNT (DISTINCT order_id) AS orders
FROM orders_on_public_holiday
GROUP BY month_of_the_year_num ;