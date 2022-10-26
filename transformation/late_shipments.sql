DROP TABLE IF EXISTS somtmoma5254_analytics.late_shipments;
CREATE TABLE somtmoma5254_analytics.late_shipments
AS
SELECT shipment_date,order_date,shipment_date - order_date  AS days_passed
FROM somtmoma5254_staging.shipments_deliveries
JOIN somtmoma5254_staging.orders ON orders.order_id = shipments_deliveries.order_id
WHERE shipments_deliveries.delivery_date IS NULL;

SELECT COUNT (DISTINCT shipment_date) AS late_shipments
FROM somtmoma5254_analytics.late_shipments
WHERE days_passed >= 6;



