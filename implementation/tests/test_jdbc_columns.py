"""F4 regression guard: JDBC staging writes must match schema.sql exactly.

Spark's JDBC writer maps columns BY NAME, so the DataFrame projected inside
pg_sink.apply_stream_batch() / pg_sink.apply_hourly_batch() must carry exactly
the column names declared for the corresponding staging table in
implementation/sql/schema.sql. A mismatch (e.g. writing Silver's
``received_at_utc`` where the table declares ``received_at``) fails the Q3
micro-batch deterministically on PostgreSQL BEFORE any upsert runs.

This suite pins both sides together using stdlib-only static parsing:
  * schema.sql  -> ordered DDL columns (regex over the CREATE TABLE block);
  * pg_sink.py  -> ordered select outputs (ast; plain strings and
    ``<expr>.alias("name")`` literals only — anything unresolvable fails the
    test so the write contract stays statically checkable);
  * common.py   -> SILVER_SCHEMA field names, to prove each select SOURCE is
    a real Silver column (catches aliasing a non-existent column).

No PostgreSQL, no PySpark, no SQLite server.
"""

import ast
import re
import sys
import unittest
from pathlib import Path

IMPL_DIR = Path(__file__).resolve().parents[1]
JOBS_DIR = IMPL_DIR / "spark" / "jobs"

sys.path.insert(0, str(JOBS_DIR))

import pg_sink  # noqa: E402,F401  (import proves the module still parses)


def _schema_columns(table):
    """Ordered column names of ``table`` from implementation/sql/schema.sql."""
    sql = (IMPL_DIR / "sql" / "schema.sql").read_text(encoding="utf-8")
    match = re.search(
        r"CREATE TABLE IF NOT EXISTS " + re.escape(table) + r"\s*\((.*?)\n\);",
        sql,
        re.DOTALL,
    )
    if match is None:
        raise AssertionError(f"table {table!r} not found in schema.sql")
    columns = []
    for line in match.group(1).splitlines():
        line = line.split("--", 1)[0].strip().rstrip(",")
        if not line:
            continue
        name = line.split()[0]
        if name.upper() in {"PRIMARY", "UNIQUE", "CONSTRAINT", "FOREIGN", "CHECK"}:
            continue
        columns.append(name)
    return columns


def _read_function(func_name):
    """AST of one top-level function in spark/jobs/pg_sink.py."""
    tree = ast.parse((JOBS_DIR / "pg_sink.py").read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == func_name:
            return node
    raise AssertionError(f"function {func_name!r} not found in pg_sink.py")


def _string_constant(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _alias_name(arg):
    """Output column of ``<expr>.alias("name")`` (the alias argument)."""
    if (isinstance(arg, ast.Call) and isinstance(arg.func, ast.Attribute)
            and arg.func.attr == "alias" and arg.args):
        return _string_constant(arg.args[0])
    return None


def _require_name(arg, func_name):
    name = _string_constant(arg) or _alias_name(arg)
    if name is None:
        raise AssertionError(
            f"pg_sink.{func_name}: JDBC select arg {ast.dump(arg)} is not a "
            "plain string or '<expr>.alias(\"<column>\")' literal — keep the "
            "staging write statically checkable (see test_jdbc_columns.py)")
    return name


def _select_outputs(func_name):
    """Ordered output column names of every df.select(...) in the function."""
    outputs = []
    for call in ast.walk(_read_function(func_name)):
        if (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
                and call.func.attr == "select"):
            outputs.extend(_require_name(arg, func_name) for arg in call.args)
    if not outputs:
        raise AssertionError(f"pg_sink.{func_name}: no df.select(...) found")
    return outputs


def _select_sources(func_name):
    """Source column name feeding each select item (unresolvable -> None)."""
    sources = []
    for call in ast.walk(_read_function(func_name)):
        if not (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
                and call.func.attr == "select"):
            continue
        for arg in call.args:
            if _string_constant(arg) is not None:
                sources.append(_string_constant(arg))          # plain string
                continue
            if _alias_name(arg) is None:
                sources.append(None)
                continue
            receiver = arg.func.value
            if isinstance(receiver, ast.Attribute):
                sources.append(receiver.attr)                  # df.col.alias
            elif isinstance(receiver, ast.Subscript):
                sl = receiver.slice                            # df["col"].alias
                sources.append(_string_constant(sl)
                               or _string_constant(getattr(sl, "value", None)))
            else:                                              # F.col("x").alias
                sources.append(next(
                    (c for c in (_string_constant(n) for n in ast.walk(receiver))
                     if c is not None), None))
    return sources


def _silver_schema_fields():
    """Field names of SILVER_SCHEMA in spark/jobs/common.py (static parse)."""
    tree = ast.parse((JOBS_DIR / "common.py").read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == "SILVER_SCHEMA"
                for t in node.targets):
            return [
                call.args[0].value
                for call in ast.walk(node.value)
                if (isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
                    and call.func.id == "StructField" and call.args
                    and _string_constant(call.args[0]) is not None)
            ]
    raise AssertionError("SILVER_SCHEMA not found in common.py")


class JdbcStagingColumnContractTests(unittest.TestCase):
    """The JDBC select output must equal the staging DDL, name for name."""

    def test_stream_staging_columns_match_schema(self):
        self.assertEqual(
            _select_outputs("apply_stream_batch"),
            _schema_columns("staging_stream_events"),
        )

    def test_hourly_staging_columns_match_schema(self):
        self.assertEqual(
            _select_outputs("apply_hourly_batch"),
            _schema_columns("staging_hourly_agg"),
        )

    def test_stream_select_sources_are_silver_columns(self):
        silver = set(_silver_schema_fields())
        for source in _select_sources("apply_stream_batch"):
            self.assertIsNotNone(
                source, "every JDBC select source must be statically resolvable")
            self.assertIn(
                source, silver,
                f"JDBC select source {source!r} is not a SILVER_SCHEMA column")


if __name__ == "__main__":
    unittest.main()
