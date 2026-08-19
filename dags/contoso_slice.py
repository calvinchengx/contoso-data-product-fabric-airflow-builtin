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
import pathlib

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
# The OneLake host and whatever else delta-rs needs to reach it, as DATA. The
# sibling leaf learned this the hard way: an `if emulator:` branch here was the
# last place the product asked which target had answered, and a deployment
# difference expressed as a branch is one the product must be edited to change.
ONELAKE_HOST = os.environ.get("FABRIC_ONELAKE_HOST", "onelake.dfs.fabric.microsoft.com")
STORAGE_OPTIONS = json.loads(os.environ.get("FABRIC_STORAGE_OPTIONS", "{}"))

# The one dataset this slice produces. `Dataset` rather than `Asset`: the
# rename landed in Airflow 3 and Fabric runs 2.10.5, so this is not a
# preference.
BRONZE = Dataset("contoso://bronze/pos_customers")
SILVER = Dataset("contoso://silver/silver_customers")
GOLD = Dataset("contoso://gold/dim_customer")

# TDS WANTS A DIFFERENT AUDIENCE. A Warehouse is Azure SQL underneath, so the
# bearer it accepts is minted for `database.windows.net` -- handing it the
# Fabric token fails as `audience not accepted`, which names neither audience.
SQL_SCOPE = "https://database.windows.net/.default"
WAREHOUSE = os.environ.get("CONTOSO_WAREHOUSE", "contoso_warehouse")

# Where the vendor's bytes are staged before they reach OneLake. A scratch
# path, not a destination: bronze lives in the Lakehouse, and a run that left
# its only copy inside the scheduler's container would have proved nothing
# about Fabric.
STAGE = "/tmp/contoso-stage"
WORKSPACE = os.environ.get("CONTOSO_WORKSPACE", "contoso-analytics")
LAKEHOUSE = os.environ.get("CONTOSO_LAKEHOUSE", "lake")
TDS_HOST = os.environ.get("FABRIC_TDS_HOST", "api.fabric.microsoft.com")
TDS_PORT = os.environ.get("FABRIC_TDS_PORT", "1433")

# The two audiences. A Fabric token opens the control plane; OneLake is ADLS
# Gen2 and wants a storage token, and using one for the other fails as a 401
# that names neither.
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
STORAGE_SCOPE = "https://storage.azure.com/.default"


def token(scope: str) -> str:
    """A bearer, from the client-credentials flow and nothing else.

    NOT AN SDK. Azure's credential chains fall through to
    DefaultAzureCredential when they do not recognise a credential shape, and
    unisolated that authenticates against REAL Microsoft endpoints with the
    developer's own login. A misconfigured SDK here does not fail -- it
    retargets production. Three lines of stdlib cannot do that.
    """
    import urllib.parse
    import urllib.request

    form = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": ENTRA_CLIENT_ID,
            "client_secret": ENTRA_CLIENT_SECRET,
            "scope": scope,
        }
    ).encode()
    request = urllib.request.Request(
        ENTRA_TOKEN_URL, data=form,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)["access_token"]


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
        os.makedirs(STAGE, exist_ok=True)
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
            path = os.path.join(STAGE, f"part-{page:04d}.csv")
            with open(path, "wb") as handle:
                handle.write(response.content)
            written += len(response.content)
        return {"pages": pages, "bytes": written}

    @task
    def provision() -> dict:
        """The workspace and Lakehouse bronze lands in.

        Idempotent by name: a second run drives what the first one made. In
        production these exist already and this task is a lookup.
        """
        session = requests.Session()
        session.headers["Authorization"] = f"Bearer {token(FABRIC_SCOPE)}"
        session.verify = False
        api = f"{FABRIC_API}/v1"

        listed = session.get(f"{api}/workspaces", timeout=60)
        listed.raise_for_status()
        workspace = next(
            (w["id"] for w in listed.json().get("value", [])
             if w.get("displayName") == WORKSPACE), "")
        if not workspace:
            made = session.post(
                f"{api}/workspaces", json={"displayName": WORKSPACE}, timeout=60)
            made.raise_for_status()
            workspace = made.json()["id"]

        items = session.get(f"{api}/workspaces/{workspace}/items", timeout=60)
        items.raise_for_status()
        lakehouse = next(
            (i["id"] for i in items.json().get("value", [])
             if i.get("displayName") == LAKEHOUSE and i.get("type") == "Lakehouse"), "")
        if not lakehouse:
            made = session.post(
                f"{api}/workspaces/{workspace}/items",
                json={"displayName": LAKEHOUSE, "type": "Lakehouse"}, timeout=120)
            made.raise_for_status()
            lakehouse = made.json()["id"]
        # THE WAREHOUSE, because gold is one. A Lakehouse holds Delta and is
        # read by Spark; a Warehouse is T-SQL over TDS and is what gold's dbt
        # models build into. Two items, because Fabric has two engines and the
        # product uses both -- which is the thing this cell has to demonstrate
        # rather than assert.
        warehouse = next(
            (i["id"] for i in items.json().get("value", [])
             if i.get("displayName") == WAREHOUSE and i.get("type") == "Warehouse"), "")
        if not warehouse:
            made = session.post(
                f"{api}/workspaces/{workspace}/items",
                json={"displayName": WAREHOUSE, "type": "Warehouse"}, timeout=180)
            made.raise_for_status()
            warehouse = made.json()["id"]
        return {"workspace": workspace, "lakehouse": lakehouse, "warehouse": warehouse}

    @task(outlets=[BRONZE])
    def to_bronze(landed: dict, where: dict) -> dict:
        """Landing → bronze, parsed and nothing else, INTO ONELAKE.

        No dedupe, no conforming, no quarantine: those are silver's, and doing
        them here would destroy the only copy of what the vendor sent.

        WRITTEN BY DELTA-RS DIRECTLY TO ONELAKE, with the bearer passed as a
        storage option. That field is the reason this is delta-rs and not an
        Azure SDK: dlt's credential model carries account-key, SAS and
        service-principal shapes and no bearer at all, so it falls through to
        DefaultAzureCredential and reaches real Microsoft endpoints.
        """
        import csv
        import glob

        import pyarrow as pa
        from deltalake import write_deltalake

        rows: list[dict] = []
        for path in sorted(glob.glob(os.path.join(STAGE, "*.csv"))):
            with open(path, newline="", encoding="utf-8") as handle:
                rows.extend(csv.DictReader(handle))
        if not rows:
            raise RuntimeError(
                f"staging at {STAGE} parsed to no rows -- the landing step "
                f"reported {landed} and bronze read nothing, so one of them is lying"
            )
        table = pa.Table.from_pylist(rows)

        uri = (
            f"abfss://{where['workspace']}@{ONELAKE_HOST}/"
            f"{where['lakehouse']}/Tables/bronze_pos_customers"
        )
        write_deltalake(
            uri, table, mode="overwrite",
            storage_options={
                "azure_storage_account_name": "onelake",
                "azure_storage_token": token(STORAGE_SCOPE),
                **STORAGE_OPTIONS,
            },
        )
        return {"rows": table.num_rows, "columns": table.num_columns, "uri": uri}

    @task
    def to_silver(bronze: dict, where: dict) -> dict:
        """Bronze → silver, with THE CORE'S MODELS. Not a copy of them.

        `silver_dir()` is a path inside the installed `contoso-data-product`
        package. dbt is pointed at it; nothing is vendored into this repo and
        nothing here restates a transform. If a model changes in core, this
        cell gets it by moving a tag -- which is the only way "one product,
        many orchestrators" is a fact rather than a slogan.

        SUBMITTED OVER LIVY. dbt-fabricspark talks to Fabric's Livy surface,
        the emulator terminates it, and the engine computes. This worker holds
        no Spark session -- an Airflow worker does not have one, which is the
        constraint that decided the whole architecture.
        """
        import subprocess
        import tempfile

        from contoso_product import silver_dir

        project = silver_dir()
        profiles = tempfile.mkdtemp()
        # The profile is DEPLOYMENT, so it is written here from the
        # environment rather than shipped: pointing dbt at a different
        # workspace must not mean editing the product.
        # THE ADAPTER'S OWN REQUIRED SHAPE, taken from the sibling leaf's
        # working profile rather than invented. A first attempt omitted
        # `livy_mode`, `authentication` and spelled the bearer `token` instead
        # of `accessToken`; dbt rejected it inside mashumaro's generated
        # deserialiser, which names the file and none of the missing fields.
        pathlib.Path(profiles, "profiles.yml").write_text(
            "contoso_silver:\n"
            "  target: dev\n"
            "  outputs:\n"
            "    dev:\n"
            "      type: fabricspark\n"
            "      method: livy\n"
            "      livy_mode: fabric\n"
            "      authentication: int_tests\n"
            f"      accessToken: {token(FABRIC_SCOPE)}\n"
            f"      endpoint: {FABRIC_API}/v1\n"
            f"      workspaceid: {where['workspace']}\n"
            f"      lakehouseid: {where['lakehouse']}\n"
            f"      lakehouse: {LAKEHOUSE}\n"
            f"      schema: {LAKEHOUSE}\n"
            "      threads: 1\n"
            "      connect_retries: 3\n"
            "      connect_timeout: 30\n"
            "      spark_config:\n"
            "        name: contoso-silver\n",
            encoding="utf-8",
        )
        env = dict(os.environ)
        env.update({
            "DBT_BRONZE_SCHEMA": LAKEHOUSE,
            "DBT_SILVER_LOCATION_ROOT":
                f"abfss://{where['workspace']}@{ONELAKE_HOST}/{where['lakehouse']}/Tables",
            # THE PLATFORM'S BRONZE NAMES. Core declares these as vars because
            # the cells genuinely disagree about what bronze is called; this
            # cell writes one table and names it here rather than renaming the
            # core's default under a running pipeline.
            "DBT_PROFILES_DIR": profiles,
        })
        run = subprocess.run(
            ["dbt", "run", "--project-dir", str(project), "--profiles-dir", profiles,
             "--select", "silver_customers",
             "--vars", json.dumps({"bronze_pos_customers": "bronze_pos_customers"})],
            env=env, capture_output=True, text=True,
        )
        print(run.stdout[-4000:])
        if run.returncode != 0:
            raise RuntimeError(f"dbt run failed ({run.returncode}):\n{run.stdout[-3000:]}\n{run.stderr[-2000:]}")
        return {"models": ["silver_customers"], "project": str(project)}

    @task
    def reflect(silver: dict, where: dict) -> dict:
        """Make silver visible to the Warehouse before gold reads it.

        SILVER LIVES IN THE LAKEHOUSE; gold is a Warehouse and reaches across
        by three-part name. The SQL analytics endpoint's view of the Lakehouse
        is a SNAPSHOT -- tables written after it was taken do not exist as far
        as T-SQL is concerned, and the failure is
        `Invalid object name '<lakehouse>.lake.silver_customers'`, which reads
        like a wrong name rather than a stale catalogue.

        ASKING IS NOT THE SAME AS CONNECTING. Opening a fresh connection
        happens to trigger a refresh on some implementations, which makes
        "just reconnect" look like a fix and does nothing against a real
        tenant, where this call is the only lever.
        """
        session = requests.Session()
        session.headers["Authorization"] = f"Bearer {token(FABRIC_SCOPE)}"
        session.verify = False
        api = f"{FABRIC_API}/v1"

        lake = session.get(
            f"{api}/workspaces/{where['workspace']}/lakehouses/{where['lakehouse']}",
            timeout=60)
        lake.raise_for_status()
        endpoint = (lake.json().get("properties", {})
                    .get("sqlEndpointProperties", {}).get("id"))
        if not endpoint:
            raise RuntimeError(
                f"lakehouse {where['lakehouse']} reports no SQL analytics endpoint; "
                f"gold has nothing to read silver through")
        refreshed = session.post(
            f"{api}/workspaces/{where['workspace']}/sqlEndpoints/{endpoint}/refreshMetadata",
            timeout=300)
        refreshed.raise_for_status()
        return {"endpoint": endpoint, "silver": silver["models"]}

    @task(outlets=[GOLD])
    def to_gold(seen: dict, where: dict) -> dict:
        """Silver → gold, with THE CORE'S SQL. Not a copy of it.

        `gold_dir()` is the installed package's dbt project, exactly as
        `silver_dir()` is. Gold is where the family's numbers come from, so a
        copy here would not merely duplicate code -- it would let this cell
        report figures no other cell could confirm.

        A WAREHOUSE, NOT A LAKEHOUSE. Fabric's Warehouse is a T-SQL engine
        reached over TDS on 1433; Spark cannot write one. dbt-fabric talks to
        it through `mssql-python`, which bundles the ODBC driver in the wheel
        -- without that this cell could not run gold at all, because the
        sidecar is Fabric's image and not one to install native drivers into.
        """
        import subprocess
        import tempfile

        from contoso_product import gold_dir

        project = gold_dir()
        profiles = tempfile.mkdtemp()
        pathlib.Path(profiles, "profiles.yml").write_text(
            "contoso_gold:\n"
            "  target: dev\n"
            "  outputs:\n"
            "    dev:\n"
            "      type: fabric\n"
            "      driver: ODBC Driver 18 for SQL Server\n"
            f"      server: {TDS_HOST}\n"
            f"      port: {TDS_PORT}\n"
            f"      database: {where['warehouse']}\n"
            "      schema: dbo\n"
            "      authentication: ActiveDirectoryAccessToken\n"
            f"      access_token: {token(SQL_SCOPE)}\n"
            "      encrypt: false\n"
            "      trust_cert: true\n"
            "      threads: 1\n",
            encoding="utf-8",
        )
        env = dict(os.environ)
        env["DBT_PROFILES_DIR"] = profiles
        env["CONTOSO_SILVER_DATABASE"] = where["lakehouse"]
        # NOT the lakehouse name. A Lakehouse's tables are exposed to T-SQL
        # through its SQL analytics endpoint under `dbo`, so the three-part
        # name gold builds is `<lakehouse-id>.dbo.silver_customers`. Setting
        # this to the lakehouse name produced
        # `Invalid object name '<id>.lake.silver_customers'` -- which survives
        # a metadata refresh, because the catalogue was never stale: the name
        # was wrong. Core already defaults this to `dbo`; overriding it was the
        # mistake.
        env["CONTOSO_SILVER_SCHEMA"] = "dbo"
        # BOTH, even though one is nominally the other's default. Core's
        # gold/models/sources.yml says
        #   env_var('CONTOSO_SILVER_DATABASE', env_var('LAKEHOUSE_ID'))
        # and Jinja evaluates arguments EAGERLY -- so the inner call runs
        # whether or not the outer variable is set, and LAKEHOUSE_ID is
        # required rather than a fallback. dbt reports it as
        # `Env var required but not provided: 'LAKEHOUSE_ID'` while the
        # variable that was supposed to make it unnecessary is right there.
        # Setting both is the honest workaround; the nested default is a core
        # defect worth fixing there.
        env["LAKEHOUSE_ID"] = where["lakehouse"]
        run = subprocess.run(
            ["dbt", "run", "--project-dir", str(project), "--profiles-dir", profiles,
             "--select", "dim_customer"],
            env=env, capture_output=True, text=True,
        )
        print(run.stdout[-4000:])
        if run.returncode != 0:
            raise RuntimeError(
                f"dbt run failed ({run.returncode}):\n{run.stdout[-3000:]}\n{run.stderr[-2000:]}")
        return {"models": ["dim_customer"], "project": str(project), "via": seen["endpoint"]}

    @task
    def report(bronze: dict, silver: dict, gold: dict) -> None:
        """State what was built, so a run says something rather than passing."""
        print(f"bronze_pos_customers: {bronze['rows']} rows, {bronze['columns']} columns")
        print(f"silver from {silver['project']}: {', '.join(silver['models'])}")
        print(f"gold from {gold['project']}: {', '.join(gold['models'])}")
        if bronze["rows"] <= 0:
            raise RuntimeError("bronze is empty")

    where = provision()
    bronze = to_bronze(land(), where)
    silver = to_silver(bronze, where)
    # reflect BETWEEN silver and gold, not beside them: gold cannot see what
    # the endpoint has not caught up with.
    seen = reflect(silver, where)
    report(bronze, silver, to_gold(seen, where))


contoso_slice()
