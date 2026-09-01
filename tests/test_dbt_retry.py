"""The dbt retry is EXECUTED here, not pattern-matched.

The rest of this suite asserts on the DAG's source text, because importing it
needs Airflow. That is fine for "does this string appear"; it is not fine for a
retry, whose entire value is in the cases where it does NOT fire. A source-text
test would pass just as happily on a retry that loops forever or one that
re-runs genuine model errors.

So the `dbt` helper and the `RECOVERED` constant are lifted out of the module by
AST and executed against a fake subprocess. No Airflow, no network, no dbt.
"""
from __future__ import annotations

import ast
import pathlib
import types

ROOT = pathlib.Path(__file__).resolve().parent.parent
SOURCE = (ROOT / "dags" / "contoso_slice.py").read_text(encoding="utf-8")


def _load():
    """Return a namespace holding just `dbt`, with subprocess faked out."""
    tree = ast.parse(SOURCE)
    wanted = [
        n for n in tree.body
        if (isinstance(n, ast.FunctionDef) and n.name == "dbt")
        or (isinstance(n, ast.Assign)
            and any(getattr(t, "id", None) == "RECOVERED" for t in n.targets))
    ]
    assert len(wanted) == 2, f"expected RECOVERED and dbt in the module, found {len(wanted)}"
    ns: dict = {}
    calls: list = []

    class FakeCompleted:
        def __init__(self, returncode, stdout):
            self.returncode, self.stdout, self.stderr = returncode, stdout, ""

    def fake_run(argv, env=None, capture_output=None, text=None, check=None):
        calls.append(argv)
        return ns["_queue"].pop(0)

    ns["subprocess"] = types.SimpleNamespace(run=fake_run)
    ns["_calls"] = calls
    ns["_Completed"] = FakeCompleted
    # exec of a fragment WE just parsed out of our own repository, with a fake
    # subprocess in scope. This is the point of the file: running the helper
    # beats asserting on its source.
    exec(compile(ast.Module(body=wanted, type_ignores=[]), "<dbt>", "exec"), ns)  # noqa: S102
    return ns


def _run(results):
    ns = _load()
    ns["_queue"] = [ns["_Completed"](rc, out) for rc, out in results]
    return ns


RECOVERED_NOTE = (
    "Database Error: [TABLE_OR_VIEW_NOT_FOUND] silver_customers\n"
    "[recovered] the engine restarted to refresh its Storage credential, which "
    "discards session state ... This statement did not run; re-run it."
)
REAL_ERROR = "Database Error in model silver_party: column 'nope' does not exist"


def test_a_clean_run_is_not_retried():
    ns = _run([(0, "Done. PASS=8")])
    ns["dbt"](["dbt", "run"], {}, "silver dbt run")
    assert len(ns["_calls"]) == 1


def test_a_real_failure_is_not_retried():
    """The whole point. A blanket retry would re-run genuine model errors and
    turn a deterministic bug into a flaky one."""
    ns = _run([(1, REAL_ERROR)])
    try:
        ns["dbt"](["dbt", "run"], {}, "silver dbt run")
    except RuntimeError as e:
        assert "silver dbt run failed (1)" in str(e)
    else:
        raise AssertionError("a real dbt failure must still raise")
    assert len(ns["_calls"]) == 1, "a failure without the marker must NOT be retried"


def test_the_recovered_marker_earns_one_retry_and_succeeds():
    ns = _run([(1, RECOVERED_NOTE), (0, "Done. PASS=8")])
    ns["dbt"](["dbt", "run"], {}, "silver dbt run")
    assert len(ns["_calls"]) == 2
    assert ns["_calls"][0] == ns["_calls"][1], "the retry must re-run the same command"


def test_it_retries_once_and_not_in_a_loop():
    """A credential refresh that keeps failing must surface, not spin: a loop
    would turn a fast failure into a job timeout."""
    ns = _run([(1, RECOVERED_NOTE), (1, RECOVERED_NOTE)])
    try:
        ns["dbt"](["dbt", "run"], {}, "gold dbt run")
    except RuntimeError as e:
        assert "gold dbt run failed (1)" in str(e)
    else:
        raise AssertionError("a repeated recovery must still raise")
    assert len(ns["_calls"]) == 2, "exactly two attempts, never more"


def test_both_call_sites_go_through_the_helper():
    """A helper nothing calls is the failure this cannot otherwise catch."""
    tree = ast.parse(SOURCE)
    calls = [n for n in ast.walk(tree)
             if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "dbt"]
    assert len(calls) == 2, f"expected silver and gold to call dbt(), found {len(calls)}"
    raw = [n for n in ast.walk(tree)
           if isinstance(n, ast.Call)
           and getattr(getattr(n.func, "value", None), "id", None) == "subprocess"
           and any(isinstance(a, ast.List)
                   and any(getattr(e, "value", None) == "run" for e in a.elts)
                   for a in n.args)]
    assert not raw, "a `dbt run` still bypasses the retry helper"
