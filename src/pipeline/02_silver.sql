-- Silver: typed and validated rows, then AUTO CDC into SCD1 (current) and SCD2 (history)
CREATE OR REFRESH STREAMING TABLE customers_clean (
  CONSTRAINT id_not_null EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT email_valid EXPECT (op = 'D' OR email LIKE '%@%') ON VIOLATION DROP ROW
)
COMMENT 'Typed CDC rows that passed data quality checks'
AS SELECT
  CAST(customer_id AS INT) AS customer_id,
  name,
  email,
  city,
  status,
  op,
  CAST(commit_time AS TIMESTAMP) AS commit_time
FROM STREAM(customers_bronze);

-- SCD Type 1: one row per customer, latest state; deletes remove the row
CREATE OR REFRESH STREAMING TABLE customers_current;

CREATE FLOW customers_current_flow AS AUTO CDC INTO customers_current
FROM STREAM(customers_clean)
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'D'
SEQUENCE BY commit_time
COLUMNS * EXCEPT (op)
STORED AS SCD TYPE 1;

-- SCD Type 2: a new version row each time city or status changes
CREATE OR REFRESH STREAMING TABLE customers_history;

CREATE FLOW customers_history_flow AS AUTO CDC INTO customers_history
FROM STREAM(customers_clean)
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'D'
SEQUENCE BY commit_time
COLUMNS * EXCEPT (op)
STORED AS SCD TYPE 2
TRACK HISTORY ON city, status;
