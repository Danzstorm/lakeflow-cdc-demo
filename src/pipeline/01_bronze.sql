-- Bronze: raw CDC rows, ingested incrementally with Auto Loader
CREATE OR REFRESH STREAMING TABLE customers_bronze
COMMENT 'Raw CDC rows from Azure SQL dbo.customers'
AS SELECT
  *,
  _metadata.file_path AS source_file,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/dbw_lakeflow_cdc/cdc_demo/landing/customers/',
  format => 'json'
);
