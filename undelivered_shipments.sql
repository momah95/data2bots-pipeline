CREATE TABLE undelivered_shipments
AS
SELECT shipments_deliveries.order_id,order_date, '2022-09-05' - order_date AS days_passed
FROM somtmoma5254_staging.shipments_deliveries
JOIN somtmoma5254_staging.orders ON orders.order_id = shipments_deliveries.order_id
WHERE shipments_deliveries.delivery_date IS NULL AND shipments_deliveries.shipment_date IS NULL;

SELECT COUNT (DISTINCT order_id) AS undelivered_shipments
FROM undelivered_shipments
WHERE days_passed >= 15;
