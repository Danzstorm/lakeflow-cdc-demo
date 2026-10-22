-- Gold: business-ready aggregates
CREATE OR REFRESH MATERIALIZED VIEW customers_by_city
COMMENT 'Active customers per city'
AS SELECT city, COUNT(*) AS active_customers
FROM customers_current
WHERE status = 'active'
GROUP BY city;

CREATE OR REFRESH MATERIALIZED VIEW customers_with_changes
COMMENT 'Customers whose city or status changed at least once'
AS SELECT customer_id, COUNT(*) AS versions
FROM customers_history
GROUP BY customer_id
HAVING COUNT(*) > 1;
