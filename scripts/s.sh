#!/usr/bin/env bash
# Run one T-SQL statement on Azure SQL without typing credentials. Usage: scripts/s.sh "SELECT ..."
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a
sqlcmd -S "$SQL_SERVER" -d "$SQL_DB" -U "$SQL_USER" -P "$SQL_PASSWORD" -Q "$1"
