#!/usr/bin/env bash
# Apply one source batch to Azure SQL, wait for CDC to capture it, then run the Lakeflow job.
# Usage: scripts/lote.sh 1|2|3
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a
export DATABRICKS_AUTH_TYPE=azure-cli

case "${1:-}" in
  1) f=sql/batch1_initial.sql ;;
  2) f=sql/batch2_changes.sql ;;
  3) f=sql/batch3_bad_data.sql ;;
  *) echo "Usage: $0 1|2|3" >&2; exit 1 ;;
esac

echo ">> Applying $f to Azure SQL"
sqlcmd -S "$SQL_SERVER" -d "$SQL_DB" -U "$SQL_USER" -P "$SQL_PASSWORD" -i "$f"
echo ">> Waiting for the CDC capture process"
sleep 15
echo ">> Running the Lakeflow job (extract -> pipeline)"
databricks bundle run customers_cdc_job
