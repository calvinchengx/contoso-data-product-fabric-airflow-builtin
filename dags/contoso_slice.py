"""Contoso on Fabric's BUILT-IN Airflow: land one vendor, then write bronze.

A VERTICAL SLICE, not the medallion. The full graph is four vendors, bronze,
eight silver models and nine gold models; this is one vendor and one bronze
table, chosen so that everything genuinely NEW about this cell is exercised
and nothing else is. What is new is not the transforms -- those are the
product's and unchanged -- it is:

  * the Airflow 2.10.5 idiom. Fabric supports exactly that version and no
    other, so this DAG cannot use the Task SDK the Airflow 3 leaf uses.
  * delivery as ITEM FILES. There is no DAG bundle here: the platform PUTs
    this file into an ApacheAirflowJob and Fabric syncs it to its scheduler.
  * running inside Fabric's own scheduler rather than one the platform owns.

WHAT THIS DAG IS NOT ALLOWED TO BECOME. When it widens to the whole medallion
the transforms still come from `contoso-data-product` -- silver's models and
gold's SQL are the core's, resolved through `silver_dir()` and `gold_dir()`.
A second copy of them here would be the defect the whole family is built to
prevent, and it would be easiest to introduce exactly here, where the DAG has
to be rewritten anyway.

THE AIRFLOW 2 DIFFERENCES, all of them, so a reader porting the rest knows the
list rather than discovering it one exception at a time:

    Airflow 3 (the sibling leaf)      Airflow 2.10.5 (here)
    from airflow.sdk import dag, task from airflow.decorators import dag, task
    Asset("contoso://bronze")         Dataset("contoso://bronze")
    Metadata(Asset(...), {...})       airflow.datasets.metadata.Metadata
    schedule=..., asset-driven        schedule=... with Dataset objects
"""

from __future__ import annotations

import json
import os

import pendulum
import requests
from airflow.datasets import Dataset
from airflow.decorators import dag, task

# THE VENDOR AND THE TARGET COME FROM THE ENVIRONMENT, never from this file.
# In production these are the real vendor's hostname and a real Fabric
# workspace; the platform supplies both, which is what makes this DAG portable
# rather than emulator-shaped.
POS_API = os.environ.get("CONTOSO_POS_API", "http://contoso-pos:8090")
POS_KEY = os.environ.get("CONTOSO_POS_API_KEY", "")
FABRIC_API = os.environ.get("FABRIC_API_ROOT", "https://fabric-emulator:9443")
ENTRA_TOKEN_URL = os.environ.get("ENTRA_TOKEN_URL", "")
ENTRA_CLIENT_ID = os.environ.get("ENTRA_CLIENT_ID", "")
ENTRA_CLIENT_SECRET = os.environ.get("ENTRA_CLIENT_SECRET", "")

# The one dataset this slice produces. `Dataset` rather than `Asset`: the
# rename landed in Airflow 3 and Fabric runs 2.10.5, so this is not a
# preference.
BRONZE = Dataset("contoso://bronze/pos_customers")

LANDING = "/opt/airflow/dags/_landing"


@dag(
    dag_id="contoso_slice",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["contoso", "fabric", "built-in-airflow"],
)
def contoso_slice():
    @task
    def land() -> dict:
        """Pull the vendor's export and write the bytes down, unchanged.

        VERBATIM, as every other cell lands it. Bronze's job is to be what
        arrived, so a question about the vendor can be answered without going
        back to the vendor.
        """
        os.makedirs(LANDING, exist_ok=True)
        # The credential is enforced by the VENDOR. Without its fixture the
        # simulator does not fail -- it generates bodies from its OpenAPI
        # schema and answers everything 200, wrong key included -- so a run
        # that skipped this check could land invented data and look fine.
        refused = requests.get(
            f"{POS_API}/api/v1/export/customers",
            headers={"X-Api-Key": "wrong-key"},
            params={"page": 1},
            timeout=120,
        )
        if refused.status_code != 401:
            raise RuntimeError(
                f"the vendor accepted a bad API key ({refused.status_code}) -- it is "
                f"serving generated data, not its fixture"
            )

        first = requests.get(
            f"{POS_API}/api/v1/export/customers",
            headers={"X-Api-Key": POS_KEY},
            params={"page": 1},
            timeout=600,
        )
        first.raise_for_status()
        pages = int(first.headers["X-Total-Pages"])
        written = 0
        for page in range(1, pages + 1):
            response = first if page == 1 else requests.get(
                f"{POS_API}/api/v1/export/customers",
                headers={"X-Api-Key": POS_KEY},
                params={"page": page},
                timeout=600,
            )
            response.raise_for_status()
            # The vendor says which page this is. Checking it catches a server
            # that ignores the parameter and returns page 1 every time, which
            # would land the right byte count and the wrong data.
            if int(response.headers["X-Page"]) != page:
                raise RuntimeError(f"asked for page {page}, got {response.headers.get('X-Page')}")
            path = os.path.join(LANDING, f"part-{page:04d}.csv")
            with open(path, "wb") as handle:
                handle.write(response.content)
            written += len(response.content)
        return {"pages": pages, "bytes": written}

    @task(outlets=[BRONZE])
    def to_bronze(landed: dict) -> dict:
        """Landing → bronze, parsed and nothing else.

        No dedupe, no conforming, no quarantine: those are silver's, and doing
        them here would destroy the only copy of what the vendor sent.
        """
        import csv
        import glob

        import pyarrow as pa
        from deltalake import write_deltalake

        rows: list[dict] = []
        for path in sorted(glob.glob(os.path.join(LANDING, "*.csv"))):
            with open(path, newline="", encoding="utf-8") as handle:
                rows.extend(csv.DictReader(handle))
        if not rows:
            raise RuntimeError(
                f"landing at {LANDING} parsed to no rows -- the landing step "
                f"reported {landed} and bronze read nothing, so one of them is lying"
            )
        table = pa.Table.from_pylist(rows)
        out = os.path.join(LANDING, "_bronze_pos_customers")
        write_deltalake(out, table, mode="overwrite")
        with open(os.path.join(LANDING, "_bronze.json"), "w", encoding="utf-8") as handle:
            json.dump({"rows": table.num_rows, "columns": table.num_columns}, handle)
        return {"rows": table.num_rows, "columns": table.num_columns}

    @task
    def report(bronze: dict) -> None:
        """State what was built, so a run says something rather than passing."""
        print(f"bronze_pos_customers: {bronze['rows']} rows, {bronze['columns']} columns")
        if bronze["rows"] <= 0:
            raise RuntimeError("bronze is empty")

    report(to_bronze(land()))


contoso_slice()
