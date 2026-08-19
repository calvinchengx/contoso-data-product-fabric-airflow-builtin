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
