#!/usr/bin/env bash
# Return the demo to its starting state: empty source, empty landing, no pipeline tables.
# Keeps all Azure resources, the schema, the Volume and the secrets.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a
export DATABRICKS_AUTH_TYPE=azure-cli MSYS_NO_PATHCONV=1
C=dbw_lakeflow_cdc.cdc_demo

echo ">> Removing the bundle (job, pipeline and its tables)"
databricks bundle destroy --auto-approve || true  # nothing to destroy on a fresh workspace
echo ">> Emptying the landing Volume"
databricks fs rm -r dbfs:/Volumes/dbw_lakeflow_cdc/cdc_demo/landing/customers 2>/dev/null || true
echo ">> Dropping leftover tables (destroy can leave them behind) and the watermark"
for t in customers_by_city customers_with_changes customers_current customers_history customers_clean customers_bronze _cdc_watermark; do
  python scripts/q.py "DROP TABLE IF EXISTS $C.$t" >/dev/null
done
echo ">> Resetting Azure SQL (empty table, CDC re-enabled)"
sqlcmd -S "$SQL_SERVER" -d "$SQL_DB" -U "$SQL_USER" -P "$SQL_PASSWORD" -i sql/reset.sql
echo ">> Redeploying the bundle"
databricks bundle deploy
echo ">> Ready: run scripts/lote.sh 1"
