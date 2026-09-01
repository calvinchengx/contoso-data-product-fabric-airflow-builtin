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
import urllib.request

import pendulum
import requests
from airflow.datasets import Dataset
from airflow.decorators import dag, task

# THE VENDOR AND THE TARGET COME FROM THE ENVIRONMENT, never from this file.
# In production these are the real vendor's hostname and a real Fabric
# workspace; the platform supplies both, which is what makes this DAG portable
# rather than emulator-shaped.
# THE VENDORS, and they agree about nothing on purpose. Three companies, three
# credentials that rotate separately, three dialects: delimited text and JSON
# Lines, JSON arrays with orders NESTED inside baskets, and binary Parquet.
# Smoothing that over in the product would be inventing a tidiness the business
# does not have -- and each awkwardness here is one a real pipeline meets.
#
# `digest` marks the vendor whose transport can corrupt a payload while leaving
# it structurally valid. Parquet keeps its PAR1 markers through byte damage, so
# only a published checksum can tell.
VENDORS = [
    {
        "name": "contoso_pos",
        "api": os.environ.get("CONTOSO_POS_API", "http://contoso-pos:8090"),
        "key": os.environ.get("CONTOSO_POS_API_KEY", ""),
        "paged": True,
        "feeds": [
            ("/api/v1/export/customers", "bronze_pos_customers", "csv"),
            ("/api/v1/export/orders", "bronze_pos_orders", "jsonl"),
        ],
    },
    {
        "name": "contoso_web",
        "api": os.environ.get("CONTOSO_WEB_API", "http://contoso-web:8091"),
        "key": os.environ.get("CONTOSO_WEB_API_KEY", ""),
        "paged": True,
        "feeds": [
            ("/api/v2/export/customers", "bronze_web_customers", "json"),
            ("/api/v2/export/products", "bronze_web_products", "json"),
            ("/api/v2/export/orders", "bronze_web_orders", "json"),
        ],
    },
    {
        "name": "contoso_reference",
        "api": os.environ.get("CONTOSO_REFERENCE_API", "http://contoso-reference:8092"),
        "key": os.environ.get("CONTOSO_REFERENCE_API_KEY", ""),
        # NOT PAGED: the whole export is about four kilobytes, and a Parquet
        # file cannot be split on line boundaries anyway.
        "paged": False,
        "digest": True,
        "feeds": [
            ("/reference/v1/product-hierarchy", "bronze_ref_product_hierarchy", "parquet"),
            ("/reference/v1/fx-rates", "bronze_ref_fx_rates", "parquet"),
        ],
    },
]
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
ERP_BROKER = os.environ.get("CONTOSO_ERP_BROKER", "contoso-erp-broker:9092")
ERP_TOPIC = os.environ.get("CONTOSO_ERP_TOPIC", "contoso.erp.customer")
# NO DEFAULT, because any default useful enough to connect would carry the
# vendor's password -- and a credential in the product is a credential in every
# clone, every reflog and every CI cache. The platform supplies this, as it
# supplies the vendors' API keys. The repo's own test caught the first version
# of this line, which is the test doing precisely its job.
ERP_DSN = os.environ.get("CONTOSO_ERP_DSN", "")
TDS_HOST = os.environ.get("FABRIC_TDS_HOST", "api.fabric.microsoft.com")
TDS_PORT = os.environ.get("FABRIC_TDS_PORT", "1433")

# The two audiences. A Fabric token opens the control plane; OneLake is ADLS
# Gen2 and wants a storage token, and using one for the other fails as a 401
# that names neither.
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
STORAGE_SCOPE = "https://storage.azure.com/.default"

# THE NAME compare_products.py IS GIVEN ON THE COMMAND LINE. Fixed here
# rather than in the platform, because the product decides what it
# publishes and the platform only decides where to put what it fetched.
SNAPSHOT_NAME = "product_snapshot.json"


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

    @task
    def land(vendor: dict) -> dict:
        """Pull one vendor's feeds and write the bytes down, unchanged.

        VERBATIM. Bronze's job is to be what arrived, so a question about the
        vendor can be answered without going back to the vendor.

        PAGED VENDORS ARE LANDED AS PARTS, not stitched back together.
        Reassembling here would put the whole 95 MB export in this process's
        memory -- the exact thing paging removes.
        """
        import hashlib

        landed = {}
        base = vendor["api"]
        headers = {"X-Api-Key": vendor["key"]}

        # THE CREDENTIAL IS THE VENDOR'S TO ENFORCE, and this proves it does.
        # Without its fixture mokapi does not fail -- it generates bodies from
        # the OpenAPI schema and answers everything 200, wrong key included --
        # so a run that skipped this could land invented data and look fine.
        probe = vendor["feeds"][0][0]
        refused = requests.get(f"{base}{probe}", headers={"X-Api-Key": "wrong-key"},
                               params={"page": 1} if vendor["paged"] else None, timeout=120)
        if refused.status_code != 401:
            raise RuntimeError(
                f"{vendor['name']} accepted a bad API key ({refused.status_code}) -- it is "
                f"serving generated data, not its fixture")

        for path, table, ext in vendor["feeds"]:
            os.makedirs(os.path.join(STAGE, table), exist_ok=True)
            if not vendor["paged"]:
                response = requests.get(f"{base}{path}", headers=headers, timeout=600)
                response.raise_for_status()
                body = response.content
                if vendor.get("digest"):
                    # PARQUET CORRUPTS QUIETLY. It keeps its PAR1 magic and
                    # footer through byte-level damage, so a ruined file passes
                    # every cheap check and fails much later inside a reader,
                    # naming neither the transport nor the cause. The vendor
                    # publishes the digest of what it sent; this is the only
                    # check that can see the difference.
                    published = response.headers.get("X-Content-SHA256", "")
                    if not published:
                        raise RuntimeError(
                            f"{path} served no X-Content-SHA256 -- this vendor's format "
                            f"corrupts silently, so an unverifiable body is not usable")
                    got = hashlib.sha256(body).hexdigest()
                    if got != published:
                        raise RuntimeError(
                            f"{path} arrived corrupted: vendor sent {published}, "
                            f"{len(body):,} bytes hash to {got}")
                    if body[:4] != b"PAR1" or body[-4:] != b"PAR1":
                        raise RuntimeError(f"{path} is not Parquet: {body[:4]!r}..{body[-4:]!r}")
                with open(os.path.join(STAGE, table, f"part-0001.{ext}"), "wb") as fh:
                    fh.write(body)
                landed[table] = {"parts": 1, "bytes": len(body), "ext": ext}
                continue

            first = requests.get(f"{base}{path}", headers=headers,
                                 params={"page": 1}, timeout=600)
            first.raise_for_status()
            pages = int(first.headers["X-Total-Pages"])
            total = 0
            for page in range(1, pages + 1):
                response = first if page == 1 else requests.get(
                    f"{base}{path}", headers=headers, params={"page": page}, timeout=600)
                response.raise_for_status()
                # The vendor says which page this is. Checking it catches a
                # server that ignores the parameter and returns page 1 every
                # time -- the right byte count and the wrong data.
                if int(response.headers["X-Page"]) != page:
                    raise RuntimeError(
                        f"{path}: asked page {page}, got {response.headers.get('X-Page')}")
                with open(os.path.join(STAGE, table, f"part-{page:04d}.{ext}"), "wb") as fh:
                    fh.write(response.content)
                total += len(response.content)
            landed[table] = {"parts": pages, "bytes": total, "ext": ext}

        return {"vendor": vendor["name"], "landed": landed}

    @task
    def land_erp() -> dict:
        """Consume the ERP change stream. The fourth vendor is not an API.

        THIS IS THE BOUNDARY. Postgres, Debezium and the broker are the world
        outside the lakehouse; everything downstream is inside it. The consumer
        is the only thing that touches both, which is exactly where a real
        ingestion job sits.

        WHAT SURVIVES AND WHAT DOES NOT. Counts survive real CDC: the same DML
        produces the same events. LSNs, commit timestamps and Kafka offsets do
        not, and nothing here asserts on them. `effective_date` travels as
        DATA, which keeps the fixture's deliberate disagreement between capture
        order and business order intact.
        """
        import time

        import pyarrow as pa
        from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

        # Debezium's op codes. `r` is a SNAPSHOT READ and must not appear: the
        # connector is registered before any DML, so an `r` here is a finding
        # about ordering rather than a row to quietly relabel.
        ops = {"c": "I", "u": "U", "d": "D"}
        columns = ["erp_customer_id", "phone", "legal_name", "account_tier", "segment",
                   "credit_band", "account_status", "payment_terms_days", "country",
                   "effective_date"]

        consumer = Consumer({
            "bootstrap.servers": ERP_BROKER,
            "group.id": "contoso-erp-builtin-airflow",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        consumer.assign([TopicPartition(ERP_TOPIC, 0, 0)])

        def watermark():
            """The high offset, or None while the topic does not exist yet.

            A MISSING TOPIC IS NOT AN ERROR, it is a vendor that has not
            finished becoming real -- the seeder is a one-shot container and
            compose does not wait for it. librdkafka answers
            `_UNKNOWN_PARTITION`, which reads like a broker fault.
            """
            try:
                _, high = consumer.get_watermark_offsets(
                    TopicPartition(ERP_TOPIC, 0), timeout=30)
            except KafkaException as exc:
                if exc.args and getattr(exc.args[0], "code", None) == KafkaError._UNKNOWN_PARTITION:
                    return None
                raise
            return high

        waited = 0.0
        while watermark() is None:
            if waited >= 300:
                raise RuntimeError(
                    f"topic {ERP_TOPIC!r} does not exist after 300s -- the ERP vendor never "
                    f"finished being seeded. The seeder registers the connector and replays "
                    f"the history; compose does not wait for it.")
            time.sleep(5)
            waited += 5

        # THE GATE, and never a sleep. A fixed wait passes on an idle machine,
        # fails on a loaded one, and -- worse -- passes with a PARTIAL stream,
        # landing a shorter file that every count stated as a minimum accepts.
        stable, last = 0, -1
        while stable < 3:
            high = watermark()
            stable = stable + 1 if high == last and high else 0
            last = high
            if stable < 3:
                time.sleep(5)
        high = last

        rows = []
        while len(rows) < high:
            msg = consumer.poll(30.0)
            if msg is None:
                raise RuntimeError(f"stream stalled at {len(rows):,}/{high:,}")
            if msg.error():
                raise RuntimeError(str(msg.error()))
            raw = msg.value()
            if raw is None:
                raise RuntimeError(
                    f"tombstone at offset {msg.offset()} -- tombstones.on.delete drifted")
            envelope = json.loads(raw)
            op = envelope["op"]
            if op not in ops:
                raise RuntimeError(
                    f"unexpected Debezium op {op!r} at offset {msg.offset()} -- 'r' means a "
                    f"snapshot read, so the connector started after the DML")
            # A delete carries its row in `before`, an insert and update in
            # `after`. REPLICA IDENTITY FULL is what makes the delete's
            # before-image complete; without it SCD2 cannot close the version
            # it belonged to and the past is silently erased.
            image = envelope["before"] if op == "d" else envelope["after"]
            if not image:
                raise RuntimeError(f"{op} at offset {msg.offset()} carried no row image")
            rows.append({"op": ops[op], "capture_offset": msg.offset(),
                         **{c: image[c] for c in columns}})
        consumer.close()

        by_op = {o: sum(1 for r in rows if r["op"] == o) for o in ("I", "U", "D")}
        # ALL THREE, because a stream carrying only inserts is a snapshot that
        # arrived over Kafka. Updates are what SCD2 is built from and deletes
        # are what close a version.
        if not all(by_op[o] > 0 for o in ("I", "U", "D")):
            raise RuntimeError(
                f"the stream carries {by_op} -- a change log missing an op class is a "
                f"snapshot with extra steps")

        # THE RECONCILIATION. Everything above proves the stream is well-formed
        # and fully read; only this proves it is COMPLETE. A connector that
        # stopped early still settles, still parses, and is simply short.
        import psycopg

        if not ERP_DSN:
            raise RuntimeError(
                "CONTOSO_ERP_DSN is not set. The reconciliation reads the ERP's own row "
                "count to prove the captured stream is complete; without it this step "
                "could only check that the stream is well-formed, which a short stream "
                "also is.")
        with psycopg.connect(ERP_DSN, connect_timeout=30) as conn:
            surviving = conn.execute("SELECT count(*) FROM erp.customer").fetchone()[0]
        net = by_op["I"] - by_op["D"]
        if net != surviving:
            longer = net > surviving
            raise RuntimeError(
                f"the captured stream implies {net:,} surviving customers "
                f"({by_op['I']:,} inserted - {by_op['D']:,} deleted) but the ERP holds "
                f"{surviving:,}. The stream is "
                + ("LONGER than the source: the history has been replayed into a topic that "
                   "still held an earlier run. The seeder truncates the TABLE, not the BROKER."
                   if longer else
                   "SHORT: Debezium did not capture the whole replay."))

        # STAGED AS PARQUET, exactly as the HTTP vendors stage their bytes --
        # and NOT returned through XCom. 93,571 change events would go into the
        # metadata database as one row, which is what XCom is explicitly not
        # for; the shape returned here is the same small manifest `land`
        # returns, so `to_bronze` treats all four vendors identically.
        import pyarrow.parquet as pq

        table = pa.table({c: pa.array([r[c] for r in rows]) for c in rows[0]})
        os.makedirs(os.path.join(STAGE, "bronze_erp_customer_changes"), exist_ok=True)
        out = os.path.join(STAGE, "bronze_erp_customer_changes", "part-0001.parquet")
        pq.write_table(table, out)
        print(f"contoso_erp: {len(rows):,} change events "
              f"({by_op['I']:,} I / {by_op['U']:,} U / {by_op['D']:,} D), "
              f"reconciled against {surviving:,} surviving rows")
        return {
            "vendor": "contoso_erp",
            "landed": {"bronze_erp_customer_changes": {
                "parts": 1, "bytes": os.path.getsize(out), "ext": "parquet"}},
        }

    @task(outlets=[BRONZE])
    def to_bronze(landed: list, erp: dict, where: dict) -> dict:
        """Landing → bronze, parsed and nothing else, into OneLake.

        No dedupe, no conforming, no quarantine -- those are silver's, and
        doing them here would destroy the only copy of what the vendor sent.

        THREE DIALECTS, THREE READERS, and none of them reshapes. Web's orders
        stay NESTED: an order carries its own `lines` array because a
        storefront thinks in baskets, and flattening is a decision that belongs
        downstream where it is visible.
        """
        import csv
        import glob

        import pyarrow as pa
        import pyarrow.parquet as pq
        from deltalake import write_deltalake

        options = {
            "azure_storage_account_name": "onelake",
            "azure_storage_token": token(STORAGE_SCOPE),
            **STORAGE_OPTIONS,
        }
        written = {}
        # THE CDC VENDOR JOINS THE OTHER THREE HERE. It reached the stage by a
        # different road -- a Kafka consumer rather than an HTTP client -- and
        # from this point it is bytes on disk like everything else, which is
        # the property that keeps bronze one step rather than two.
        for result in list(landed) + [erp]:
            for table, meta in result["landed"].items():
                paths = sorted(glob.glob(os.path.join(STAGE, table, f"*.{meta['ext']}")))
                if meta["ext"] == "parquet":
                    tbl = pa.concat_tables([pq.read_table(p) for p in paths])
                elif meta["ext"] == "csv":
                    rows = []
                    for path in paths:
                        with open(path, newline="", encoding="utf-8") as fh:
                            rows.extend(csv.DictReader(fh))
                    tbl = pa.Table.from_pylist(rows)
                elif meta["ext"] == "jsonl":
                    rows = []
                    for path in paths:
                        with open(path, encoding="utf-8") as fh:
                            rows.extend(json.loads(line) for line in fh if line.strip())
                    tbl = pa.Table.from_pylist(rows)
                else:  # json arrays, one self-contained array per page
                    rows = []
                    for path in paths:
                        with open(path, encoding="utf-8") as fh:
                            body = json.load(fh)
                        rows.extend(body if isinstance(body, list) else [body])
                    tbl = pa.Table.from_pylist(rows)

                if tbl.num_rows == 0:
                    raise RuntimeError(
                        f"{table} parsed to no rows from {len(paths)} part(s) -- landing "
                        f"reported {meta}, so one of them is lying")
                uri = (f"abfss://{where['workspace']}@{ONELAKE_HOST}/"
                       f"{where['lakehouse']}/Tables/{table}")
                write_deltalake(uri, tbl, mode="overwrite", storage_options=options)
                written[table] = {"rows": tbl.num_rows, "columns": tbl.num_columns}
                print(f"{table}: {tbl.num_rows} rows, {tbl.num_columns} columns")
        return written

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
            # FOUR, not one. Seven of core's eight silver models depend only on
            # sources -- only silver_party refs another -- so dbt can build
            # seven at once, and the emulator does not serialise them: it
            # terminates Livy itself and each statement reaches Sail
            # independently. Measured on fabric-emulator's
            # medallion-dbt-fabricspark example, same adapter and engine: at 1
            # the models START 26s and 15s apart and the step takes 96.0s; at 4
            # they start in the SAME SECOND and it takes 45.5s. Carried over
            # from where it was measured rather than tuned here -- this graph
            # admits seven, so the ceiling is untested. The gold profile below
            # keeps threads: 1, being a different adapter over TDS.
            "      threads: 4\n"
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
        # EVERY MODEL, and no --select. dbt's own graph decides the order,
        # which is the reason for pointing it at a project rather than issuing
        # statements: silver_party reads silver_customers and
        # silver_web_customers, and stating that order here would be a second
        # place for the graph to live.
        #
        # The bronze names go in as vars because the cells genuinely disagree
        # about what bronze is called -- core declares them as vars precisely
        # so a platform can say. This cell writes the vendor-prefixed names, so
        # these are identities; a cell whose bronze predates the convention
        # maps them here instead of renaming tables under a running pipeline.
        run = subprocess.run(
            ["dbt", "run", "--project-dir", str(project), "--profiles-dir", profiles,
             "--vars", json.dumps({name: name for name in (
                 "bronze_pos_customers", "bronze_pos_orders",
                 "bronze_web_customers", "bronze_web_orders", "bronze_web_products",
                 "bronze_ref_product_hierarchy", "bronze_ref_fx_rates",
                 "bronze_erp_customer_changes")})],
            env=env, capture_output=True, text=True, check=False,
        )
        print(run.stdout[-4000:])
        if run.returncode != 0:
            raise RuntimeError(f"dbt run failed ({run.returncode}):\n{run.stdout[-3000:]}\n{run.stderr[-2000:]}")
        built = sorted(p.stem for p in (project / "models").glob("*.sql"))
        return {"models": built, "project": str(project)}

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
        # DBT_-PREFIXED SINCE CORE v0.6.0. Snowflake's dbt Projects refuse any
        # env var key that is not UPPERCASE and DBT_-prefixed, so the name this
        # used to set could not be supplied there at all -- gold ran on every
        # engine in this family except the one named for running dbt as a
        # first-class object.
        env["DBT_SILVER_DATABASE"] = where["lakehouse"]
        # NOT the lakehouse name. A Lakehouse's tables are exposed to T-SQL
        # through its SQL analytics endpoint under `dbo`, so the three-part
        # name gold builds is `<lakehouse-id>.dbo.silver_customers`. Setting
        # this to the lakehouse name produced
        # `Invalid object name '<id>.lake.silver_customers'` -- which survives
        # a metadata refresh, because the catalogue was never stale: the name
        # was wrong. Core already defaults this to `dbo`; overriding it was the
        # mistake.
        env["DBT_SILVER_SCHEMA"] = "dbo"
        # THE CORE DEFECT WAS FIXED, so the workaround is gone. This used to set
        # LAKEHOUSE_ID as well, because gold said
        #   env_var('CONTOSO_SILVER_DATABASE', env_var('LAKEHOUSE_ID'))
        # and Jinja evaluates arguments EAGERLY -- so the inner call ran whether
        # or not the outer variable was set, and dbt reported
        # `Env var required but not provided: 'LAKEHOUSE_ID'` while the variable
        # meant to make it unnecessary sat right there. The comment here called
        # it "a core defect worth fixing there", and v0.6.0 fixed it: the nested
        # default is gone and nothing reads LAKEHOUSE_ID for dbt any more.
        # Fabric's own LAKEHOUSE_ID, which notebookutils reads, is a different
        # thing and untouched.
        run = subprocess.run(
            ["dbt", "run", "--project-dir", str(project), "--profiles-dir", profiles],
            env=env, capture_output=True, text=True, check=False,
        )
        print(run.stdout[-4000:])
        if run.returncode != 0:
            raise RuntimeError(
                f"dbt run failed ({run.returncode}):\n{run.stdout[-3000:]}\n{run.stderr[-2000:]}")

        # THE CONTRACTS, ACTUALLY RUN. Publishing a list of guarantees this
        # runtime never evaluated is worse than publishing none: another cell's
        # snapshot names the same five, and comparing the two would say they
        # agree when only one of them checked. `dbt test` is what makes the
        # names mean something here.
        tested = subprocess.run(
            ["dbt", "test", "--project-dir", str(project), "--profiles-dir", profiles],
            env=env, capture_output=True, text=True, check=False,
        )
        print(tested.stdout[-4000:])
        if tested.returncode != 0:
            raise RuntimeError(
                f"gold's contracts failed ({tested.returncode}):\n"
                f"{tested.stdout[-3000:]}\n{tested.stderr[-2000:]}")

        # THE VERDICTS, FROM THE RUN'S OWN ARTEFACT. Globbing `tests/*.sql`
        # names what this project CONTAINS; run_results.json names what this
        # invocation EVALUATED. They are the same list only when nothing went
        # wrong, and the whole reason to publish contract names is the case
        # where something did.
        results = project / "target" / "run_results.json"
        if not results.exists():
            raise RuntimeError(
                f"dbt test exited 0 but wrote no {results} -- refusing to "
                f"guess whether the contracts passed.")
        payload = json.loads(results.read_text(encoding="utf-8"))
        # ASSERT WHICH INVOCATION WROTE IT. `dbt run` shares this target
        # directory and overwrites the file, so it is the contracts' verdict
        # only if `dbt test` wrote it last. The sibling platform found this the
        # expensive way: a `run` artefact reports nine models and zero
        # failures, which believed publishes "no contract failures" for a run
        # where contracts failed.
        which = (payload.get("args") or {}).get("which")
        if which != "test":
            raise RuntimeError(
                f"{results} was written by `dbt {which}`, not `dbt test` -- "
                f"refusing to report contract results from another command's "
                f"artefact.")

        evaluated, failures = set(), []
        for r in payload.get("results", []):
            uid = r.get("unique_id", "")
            name = uid.split(".")[2] if uid.count(".") >= 2 else uid
            evaluated.add(name)
            if r.get("status") in ("pass", "success"):
                continue
            failures.append({"contract": name, "status": r.get("status"),
                             "failures": r.get("failures"),
                             "detail": (r.get("message") or "").strip()[:200]})

        built = sorted(p.stem for p in (project / "models").glob("*.sql"))
        # The five named contracts, CHECKED AGAINST THE RUN. A name that is on
        # disk but absent from run_results was not evaluated, and publishing it
        # would let this cell appear to assert a guarantee it never tested --
        # `compare_products` would then read agreement between a runtime that
        # checked and one that did not.
        contracts = sorted(p.stem for p in (project / "tests").glob("*.sql"))
        unevaluated = [c for c in contracts if c not in evaluated]
        if unevaluated:
            raise RuntimeError(
                f"gold's tests/ names {', '.join(unevaluated)} but this "
                f"`dbt test` evaluated no such test -- the snapshot would "
                f"claim a guarantee that was never checked.")
        return {"models": built, "contracts": contracts, "failures": failures,
                "project": str(project), "via": seen["endpoint"],
                "warehouse": where["warehouse"]}

    @task
    def snapshot(gold: dict, where: dict) -> dict:
        """The three aggregates `compare_products.py` holds every runtime to.

        THE FAMILY'S CLAIM IS NOT THAT SIX PIPELINES ARE GREEN. It is that they
        build the same product, and only the same numbers establish that. This
        cell ran the whole medallion and matched the family to the last decimal
        place, and none of that counted, because the figures lived in a task
        log that a human had to read. A number nobody can diff is a number
        nobody checked.

        DELIBERATELY THE DUMBEST POSSIBLE SQL, and deliberately NOT through
        dbt. A comparison whose two sides share machinery proves only that the
        machinery agrees with itself; if the adapter that built the star also
        reported its total, an adapter bug would cancel out exactly where it
        matters. This reads the star directly over TDS.
        """
        import pyodbc

        raw = token(SQL_SCOPE).encode("utf-16-le")
        dsn = ("DRIVER={ODBC Driver 18 for SQL Server};"
               f"SERVER={TDS_HOST},{TDS_PORT};DATABASE={gold['warehouse']};"
               "Encrypt=no;TrustServerCertificate=yes")
        # SQL_COPT_SS_ACCESS_TOKEN. The bearer goes in as a length-prefixed
        # UTF-16LE blob on a connection attribute, not in the DSN -- the same
        # shape `fabric-platform-notebook-pipelines` uses, because this is its
        # warehouse too and inventing a second way to reach it is how the last
        # three configuration mistakes happened.
        attrs = {1256: len(raw).to_bytes(4, "little") + raw}
        with pyodbc.connect(dsn, attrs_before=attrs, timeout=30) as conn:
            row = conn.cursor().execute(
                "SELECT COALESCE(SUM(revenue_usd), 0), "
                "COALESCE(SUM(cancelled_revenue_usd), 0), "
                "COALESCE(SUM(sale_lines), 0) FROM dbo.fct_revenue_summary"
            ).fetchone()
        if row is None:
            # "COULD NOT READ" IS NOT "ZERO". Defaulting here once published a
            # snapshot claiming a runtime built nothing while dbt had just
            # reported nine models built, and compare_products refused it as an
            # empty runtime -- the right call on the evidence, the wrong
            # diagnosis. The read was blind; the warehouse was full.
            raise RuntimeError(
                "gold built, but its aggregates came back with no rows -- "
                "refusing to publish a snapshot of zeros.")

        # STRINGS, NOT FLOATS. The warehouse stores money as decimal(19,4);
        # through a JSON number the exact digits would not survive, and a
        # precision artefact would read as two runtimes disagreeing about
        # revenue. No cast is needed on this engine -- unlike the Databricks
        # cell, pyodbc hands back Decimal and str() is exact.
        snap = {
            "revenue_usd": str(row[0]),
            "cancelled_revenue_usd": str(row[1]),
            "sale_lines": str(row[2]),
            "contracts": gold["contracts"],
            "runtime": "fabric-airflow-builtin",
            "catalog": gold["warehouse"],
        }
        # ABSENT WHEN CLEAN, rather than an empty list on every green snapshot.
        # An always-present `[]` makes "evaluated its contracts and they
        # passed" indistinguishable from "never checked", which is the one
        # distinction this field exists to carry.
        if gold["failures"]:
            snap["contract_failures"] = gold["failures"]

        body = (json.dumps(snap, indent=2) + "\n").encode("utf-8")
        # PUBLISHED TO ONELAKE, not left in the worker. The DAG runs inside
        # Fabric's Airflow with no bind mount to anywhere a comparison could
        # read, so the product writes its evidence to its own lakehouse and the
        # platform fetches it. That split is the point: the product knows what
        # it measured, the platform knows how to reach its own storage.
        #
        # ONE PUT, not the DFS create/append/flush dance. Measured against this
        # emulator: `?resource=file` creates (201) but `?action=append` and
        # `?action=flush` both answer 405 UnsupportedHttpVerb -- it serves the
        # Blob API here. A single BlockBlob PUT round-trips, and the snapshot
        # is a few hundred bytes, so nothing needs chunking.
        endpoint = STORAGE_OPTIONS.get(
            "azure_endpoint", f"https://{ONELAKE_HOST}").rstrip("/")
        url = (f"{endpoint}/{where['workspace']}/{where['lakehouse']}"
               f"/Files/{SNAPSHOT_NAME}")
        request = urllib.request.Request(
            url, data=body, method="PUT",
            headers={"Authorization": f"Bearer {token(STORAGE_SCOPE)}",
                     "x-ms-blob-type": "BlockBlob",
                     "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            if response.status not in (200, 201):
                raise RuntimeError(
                    f"publishing {SNAPSHOT_NAME} answered {response.status}")
        print(f"gold snapshot -> {url}")
        print(json.dumps(snap, indent=2))
        return snap

    @task
    def report(bronze: dict, silver: dict, gold: dict, snap: dict) -> None:
        """State what was built, so a run says something rather than passing."""
        for table, meta in sorted(bronze.items()):
            print(f"{table}: {meta['rows']} rows, {meta['columns']} columns")
        print(f"silver from {silver['project']}: {', '.join(silver['models'])}")
        print(f"silver: {len(silver['models'])} models -- {', '.join(silver['models'])}")
        print(f"gold: {len(gold['models'])} models -- {', '.join(gold['models'])}")
        print(f"contracts: {', '.join(gold['contracts'])}")
        print(f"revenue_usd: {snap['revenue_usd']}")
        print(f"cancelled_revenue_usd: {snap['cancelled_revenue_usd']}")
        print(f"sale_lines: {snap['sale_lines']}")
        if not bronze:
            raise RuntimeError("bronze is empty")

    where = provision()
    # ONE land TASK PER VENDOR, expanded from the declaration. Three vendors do
    # not know about each other -- which is exactly why resolving them into one
    # customer downstream is hard -- so they fan out and bronze joins them.
    bronze = to_bronze(land.expand(vendor=VENDORS), land_erp(), where)
    silver = to_silver(bronze, where)
    # reflect BETWEEN silver and gold, not beside them: gold cannot see what
    # the endpoint has not caught up with.
    seen = reflect(silver, where)
    gold = to_gold(seen, where)
    # THE SNAPSHOT IS PART OF THE RUN, not something a human reads out of a
    # log afterwards. A cell whose numbers are only ever quoted by hand cannot
    # be compared against its siblings, which is the one thing the family is
    # for.
    report(bronze, silver, gold, snapshot(gold, where))


contoso_slice()
