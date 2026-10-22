"""Run one SQL statement on the demo serverless warehouse and print rows. Usage: python scripts/q.py "SELECT 1" """
import json
import os
import subprocess
import sys

from pathlib import Path

# Load .env (KEY=VALUE lines) so the script also works outside `set -a; . ./.env`.
envfile = Path(__file__).resolve().parent.parent / ".env"
if envfile.exists():
    for line in envfile.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

WAREHOUSE = os.environ["DATABRICKS_WAREHOUSE_ID"]
env = {**os.environ, "DATABRICKS_AUTH_TYPE": "azure-cli"}  # DATABRICKS_HOST comes from .env
body = {"warehouse_id": WAREHOUSE, "statement": sys.argv[1], "wait_timeout": "50s"}
out = subprocess.run(
    ["databricks", "api", "post", "/api/2.0/sql/statements", "--json", json.dumps(body)],
    capture_output=True, text=True, env=env, shell=os.name == "nt",
)
res = json.loads(out.stdout)
if res["status"]["state"] != "SUCCEEDED":
    sys.exit(json.dumps(res["status"], indent=2))
if not res["manifest"]["schema"].get("columns"):  # DDL returns no result set
    sys.exit(print("OK"))
cols = [c["name"] for c in res["manifest"]["schema"]["columns"]]
print(" | ".join(cols))
for row in res.get("result", {}).get("data_array", []):
    print(" | ".join("" if v is None else str(v) for v in row))
