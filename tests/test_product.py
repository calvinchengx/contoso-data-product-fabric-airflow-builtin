"""Product tests: no platform, no emulator, no credentials."""

from __future__ import annotations

import ast
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
SOURCE = (ROOT / "dags" / "contoso_slice.py").read_text(encoding="utf-8")


def code() -> str:
    """The DAG with its prose removed -- docstrings and comments both.

    THESE ASSERTIONS ARE ABOUT CODE, and the first versions of three of them
    failed on the DAG's own documentation: its module docstring carries a table
    comparing the Airflow 3 idiom with the Airflow 2 one, so it necessarily
    contains `airflow.sdk`, `Asset` and `Metadata` -- the exact strings the
    tests forbid. A test that cannot tell a construct from the sentence
    explaining why that construct is wrong will fire on good documentation and
    push the next person to delete it.

    Comments go too, for the same reason and because a comment naming a
    forbidden pattern is how these rules get explained.
    """
    tree = ast.parse(SOURCE)
    prose = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            doc = ast.get_docstring(node, clean=False)
            if doc:
                prose.add(doc)
    stripped = SOURCE
    for doc in prose:
        stripped = stripped.replace(doc, "")
    return "\n".join(
        line for line in stripped.splitlines() if not line.lstrip().startswith("#")
    )


DAG = code()


def test_the_dag_uses_the_airflow_2_idiom_fabric_actually_runs():
    """Fabric supports Airflow 2.10.5 and no other version.

    The Task SDK (`airflow.sdk`) and `Asset` arrived in Airflow 3. A DAG
    written in the sibling leaf's idiom would import cleanly on a developer's
    Airflow 3 and fail inside Fabric, which is the worst place to find out --
    the failure surfaces as a DAG that never appears rather than an error
    anyone reads.
    """
    assert "from airflow.decorators import dag, task" in DAG
    assert "from airflow.datasets import Dataset" in DAG
    assert "airflow.sdk" not in DAG, "airflow.sdk is Airflow 3 only; Fabric runs 2.10.5"
    tree = ast.parse(DAG)
    names = {
        node.id for node in ast.walk(tree) if isinstance(node, ast.Name)
    } | {
        node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
    }
    assert "Asset" not in names, "Asset is Airflow 3's name for Dataset"


def test_no_deployment_literal_is_baked_into_the_dag():
    """Endpoints and credentials come from the environment, never from here.

    The platform supplies them, and in production they are a real vendor's
    hostname and a real workspace. A literal here would make this DAG
    emulator-shaped -- runnable in one place and nowhere else -- which is
    exactly what a portable product must not be.
    """
    tree = ast.parse(SOURCE)

    # THE NAME OF AN ENVIRONMENT VARIABLE IS NOT A CREDENTIAL. This flagged
    # `ENTRA_CLIENT_SECRET` on its first run -- which is the string that says
    # "I read a secret from here", the correct pattern, and precisely what a
    # DAG should contain instead of the value. Collected from the AST rather
    # than guessed at by shape, so the exemption is exact: these are literally
    # the keys passed to os.environ.get.
    env_keys = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if (
            isinstance(func, ast.Attribute)
            and func.attr == "get"
            and isinstance(func.value, ast.Attribute)
            and func.value.attr == "environ"
            and node.args
            and isinstance(node.args[0], ast.Constant)
        ):
            env_keys.add(node.args[0].value)

    # A DICT KEY IS A FIELD NAME, NOT A CREDENTIAL. `{"client_secret": VAR}` is
    # the OAuth parameter every client-credentials request must send; the
    # secret is the VALUE beside it, and here that value is a variable read
    # from the environment. Flagging the key forbade the protocol itself.
    #
    # This is the distinction that makes the rule checkable: a leaked
    # credential is a string LITERAL in value position. `{"client_secret":
    # "hunter2"}` is still caught, because the value is a literal.
    key_strings = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Dict):
            for key in node.keys:
                if isinstance(key, ast.Constant) and isinstance(key.value, str):
                    key_strings.add(key.value)

    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            doc = ast.get_docstring(node, clean=False)
            if doc:
                docstrings.add(doc)
    offenders = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if node.value in docstrings or node.value in env_keys or node.value in key_strings:
                continue
            low = node.value.lower()
            if any(bad in low for bad in ("secret", "password", "api-key=")):
                offenders.append(node.value)
    assert offenders == [], f"a credential is written into the DAG: {offenders}"


def test_the_vendor_credential_is_proved_before_the_data_is_trusted():
    """Without its fixture the simulator answers 200 to a wrong key.

    A run that skipped this check could land schema-generated data and look
    entirely fine -- green pipeline, plausible row counts, nothing true.
    """
    assert "wrong-key" in DAG
    assert "401" in DAG


def test_bronze_parses_and_does_nothing_else():
    """No dedupe, no conforming, no quarantine -- those are silver's.

    Doing them here destroys the only copy of what the vendor sent.
    """
    for forbidden in ("drop_duplicates", "distinct(", "quarantine", "row_number"):
        assert forbidden not in DAG, f"bronze is doing silver's work: {forbidden}"


def test_the_slice_declares_what_it_produces():
    """An outlet, so widening this DAG inherits a lineage habit rather than
    acquiring one later."""
    assert "outlets=[BRONZE]" in DAG
    assert 'Dataset("contoso://bronze/pos_customers")' in DAG


def test_silver_comes_from_the_core_and_is_not_restated_here():
    """The models are `silver_dir()`'s, resolved at run time.

    THE ONE RULE THIS CELL IS MOST LIKELY TO BREAK. Widening a DAG to silver
    means rewriting orchestration, and copying the models in while you are
    there is the path of least resistance -- it would work, it would pass, and
    it would end "one product, many orchestrators" quietly. So the check is
    structural: this repo must contain no dbt project of its own, and the DAG
    must resolve the core's.
    """
    assert "from contoso_product import silver_dir" in DAG
    assert "silver_dir()" in DAG
    stray = [
        str(p.relative_to(ROOT))
        for p in ROOT.rglob("*.sql")
        if ".venv" not in p.parts
    ] + [
        str(p.relative_to(ROOT))
        for p in ROOT.rglob("dbt_project.yml")
        if ".venv" not in p.parts
    ]
    assert stray == [], f"a transform was copied into this leaf: {stray}"


def test_the_dbt_profile_is_written_from_the_environment():
    """A profile is DEPLOYMENT. Shipping one would mean editing the product to
    point it at a different workspace, which is what a portable product must
    never require."""
    assert "profiles.yml" in DAG
    assert "where['workspace']" in DAG and "where['lakehouse']" in DAG


def test_the_dag_declares_every_task_the_medallion_needs():
    """The task SET, asserted -- because nothing else here checks it.

    While widening this DAG a text edit silently deleted `provision` and
    `to_silver`: the replaced span ran from one function to another and the two
    in between went with it. Every existing test still passed, because they
    checked idioms and imports rather than the graph. A DAG missing a task is
    not a syntax error -- it parses, it runs, and it quietly does less.
    """
    import ast

    tree = ast.parse(SOURCE)
    defined = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
    }
    for task in ("provision", "land", "to_bronze", "to_silver", "reflect",
                 "to_gold", "snapshot", "report"):
        assert task in defined, f"the DAG has lost its {task} task"


def test_every_vendor_is_landed_and_none_is_hard_coded():
    """Three vendors, each with its own credential and its own dialect.

    A key that worked across vendors would prove nothing about any of them:
    these are separate companies whose credentials rotate separately, which is
    what having three vendors means rather than three routes on one.
    """
    assert "VENDORS = [" in DAG
    for vendor in ("contoso_pos", "contoso_web", "contoso_reference"):
        assert vendor in DAG, vendor
    assert "land.expand(vendor=VENDORS)" in DAG, "vendors must fan out, not be listed by hand"
    # The reference vendor's transport corrupts silently; only its published
    # digest can tell, so the check has to be there.
    assert "X-Content-SHA256" in DAG
    assert "PAR1" in DAG


def test_no_model_selector_narrows_the_medallion():
    """Every model, and dbt's own graph decides the order.

    A `--select` was right while this was a slice and is wrong now: it would
    silently build a subset while the run still reports success, and the
    numbers would simply be smaller. The graph lives in the dbt project --
    silver_party reads silver_customers and silver_web_customers -- so
    restating any part of it here would be a second place for it to live.
    """
    assert "--select" not in DAG, (
        "a model selector narrows the build; the whole medallion is the point"
    )


def test_gold_runs_its_contracts_rather_than_naming_them():
    """A snapshot listing contracts this runtime never evaluated is worse than
    one listing none.

    Another cell's snapshot names the same five, so comparing the two would
    report agreement when only one of them checked. `dbt test` is what makes
    the names mean something here, and a failure has to fail the task.
    """
    assert '"dbt", "test"' in DAG
    assert "gold's contracts failed" in DAG


def test_the_run_publishes_a_snapshot_the_family_can_compare():
    """A number nobody can diff is a number nobody checked.

    This cell ran the entire medallion and matched the family to the last
    decimal place, and none of it counted toward DoD 4, because the figures
    only ever existed in a task log a human had to read. The comparison is the
    point of the family; a cell that cannot be compared has not finished.
    """
    for key in ("revenue_usd", "cancelled_revenue_usd", "sale_lines"):
        assert key in DAG, f"the snapshot must carry {key}"
    assert "SNAPSHOT_NAME" in DAG
    # Published to storage rather than left in the worker: the DAG runs inside
    # Fabric's Airflow with no bind mount anywhere a comparison could read it.
    assert "x-ms-blob-type" in DAG


def test_the_snapshot_does_not_read_the_star_through_dbt():
    """Two sides sharing machinery prove only that the machinery agrees.

    If the adapter that built gold also reported gold's total, an adapter
    defect would cancel out exactly where the family is looking. The snapshot
    reads the star directly over TDS.
    """
    body = DAG[DAG.index("def snapshot("):DAG.index("def report(")]
    assert "pyodbc" in body
    assert "dbt" not in body, "the snapshot must not go through the adapter that built gold"
    assert "fct_revenue_summary" in body


def test_contract_names_come_from_the_run_not_the_filesystem():
    """`tests/*.sql` names what the project CONTAINS, not what ran.

    They are the same list only when nothing went wrong -- and the reason to
    publish contract names at all is the case where something did. dbt shares
    one target directory between `run` and `test`, so the artefact is the
    contracts' verdict only if `dbt test` wrote it last.
    """
    assert "run_results.json" in DAG
    assert 'which != "test"' in DAG, (
        "a `dbt run` artefact reports zero contract failures, which believed "
        "publishes a green snapshot for a run whose contracts failed"
    )
    assert "unevaluated" in DAG


def test_an_unreadable_star_is_not_reported_as_zero():
    """`compare_products` refuses an all-zero snapshot, so a blind read must
    not be allowed to look like an empty warehouse.

    The sibling cell published exactly that once: zeros from a read that
    returned no rows, while dbt had just reported nine models built.
    """
    assert "refusing to publish a snapshot of zeros" in DAG
