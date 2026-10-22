"""Incremental CDC extractor: Azure SQL cdc.dbo_customers_CT -> JSON files in a Unity Catalog Volume."""
from datetime import datetime, timezone

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

CATALOG, SCHEMA = "dbw_lakeflow_cdc", "cdc_demo"
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing/customers"
WATERMARK = f"{CATALOG}.{SCHEMA}._cdc_watermark"

spark = SparkSession.builder.getOrCreate()
secrets = DBUtils(spark).secrets

spark.sql(f"CREATE TABLE IF NOT EXISTS {WATERMARK} (last_lsn STRING, updated_at TIMESTAMP)")
row = spark.sql(f"SELECT last_lsn FROM {WATERMARK} ORDER BY updated_at DESC LIMIT 1").first()
last_lsn = row.last_lsn if row else None

lower = f"AND c.__$start_lsn > CONVERT(binary(10), '{last_lsn}', 1)" if last_lsn else ""
# __$operation: 1 = delete, 2 = insert, 3 = update (before image, skipped), 4 = update (after image)
query = f"""(
  SELECT CONVERT(varchar(22), c.__$start_lsn, 1) AS lsn,
         CONVERT(varchar(22), c.__$seqval, 1)    AS seqval,
         CASE c.__$operation WHEN 1 THEN 'D' WHEN 2 THEN 'I' ELSE 'U' END AS op,
         c.customer_id, c.name, c.email, c.city, c.status, c.updated_at,
         sys.fn_cdc_map_lsn_to_time(c.__$start_lsn) AS commit_time
  FROM cdc.dbo_customers_CT c
  WHERE c.__$operation IN (1, 2, 4)
    AND c.__$start_lsn <= sys.fn_cdc_get_max_lsn()
    {lower}
) q"""

df = (
    spark.read.format("sqlserver")
    .option("host", secrets.get("sql_demo", "host"))
    .option("port", "1433")
    .option("database", secrets.get("sql_demo", "db"))
    .option("user", secrets.get("sql_demo", "user"))
    .option("password", secrets.get("sql_demo", "password"))
    .option("dbtable", query)
    .load()
)

# Write first, then derive count and watermark from what was actually written
# (serverless has no cache(), and a second read from SQL could see newer changes).
batch_path = f"{LANDING}/batch_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
df.coalesce(1).write.mode("overwrite").json(batch_path)
try:
    written = spark.read.json(batch_path)
    n = written.count()
except AnalysisException:  # no rows written, nothing to infer a schema from
    n = 0
if n == 0:
    print("No new changes.")
else:
    new_lsn = written.agg(F.max("lsn")).first()[0]  # fixed-width hex string, sorts like the binary LSN
    spark.sql(f"INSERT INTO {WATERMARK} VALUES ('{new_lsn}', current_timestamp())")
    print(f"Wrote {n} change rows to {batch_path}, watermark -> {new_lsn}")
