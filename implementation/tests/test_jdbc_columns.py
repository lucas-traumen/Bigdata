"""F4 regression guard: JDBC staging writes must match schema.sql exactly.

Spark's JDBC writer maps columns BY NAME, so the DataFrame projected inside
pg_sink.apply_stream_batch() / pg_sink.apply_hourly_batch() must carry exactly
the column names declared for the corresponding staging table in
implementation/sql/schema.sql. A mismatch (e.g. writing Silver's
``received_at_utc`` where the table declares ``received_at``) fails the Q3
micro-batch deterministically on PostgreSQL BEFORE any upsert runs.

This suite pins both sides together using stdlib-only static parsing:
  * schema.sql  -> ordered DDL columns (regex over the CREATE TABLE block);
  * pg_sink.py  -> ordered select outputs (ast; plain strings,
    ``<expr>.alias("name")`` literals and ``*METRIC_NAMES``, which is
    resolved statically to the METRIC_CONFIG registry keys of common.py —
    anything unresolvable fails the test so the write contract stays
    statically checkable);
  * common.py   -> silver_schema() field names + METRIC_CONFIG keys, to prove
    each select SOURCE is a real Silver column and that the metric dimension
    follows the single-source registry.

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


def _read_function(module_name, func_name):
    """AST of one top-level function in a spark/jobs module."""
    tree = ast.parse((JOBS_DIR / module_name).read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == func_name:
            return node
    raise AssertionError(f"function {func_name!r} not found in {module_name}")


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


def _starred_names(arg, func_name):
    """Columns contributed by a ``*METRIC_NAMES`` select arg (None otherwise).

    The star is resolved statically to the METRIC_CONFIG registry keys, so
    the guard still pins the FULL output column list against the staging DDL:
    adding a metric to the registry now automatically extends the expected
    columns, and any registry/DDL drift still fails the test loudly.
    """
    if not isinstance(arg, ast.Starred):
        return None
    if isinstance(arg.value, ast.Name) and arg.value.id == "METRIC_NAMES":
        return list(_metric_config_keys())
    raise AssertionError(
        f"pg_sink.{func_name}: starred select arg {ast.dump(arg)} must be "
        "*METRIC_NAMES (the metric registry of common.py) so the guard can "
        "resolve it statically (see test_jdbc_columns.py)")


def _require_name(arg, func_name):
    """Output column(s) of one select argument, as a list."""
    starred = _starred_names(arg, func_name)
    if starred is not None:
        return starred
    name = _string_constant(arg) or _alias_name(arg)
    if name is None:
        raise AssertionError(
            f"pg_sink.{func_name}: JDBC select arg {ast.dump(arg)} is not a "
            "plain string, an '<expr>.alias(\"<column>\")' literal or "
            "*METRIC_NAMES — keep the staging write statically checkable "
            "(see test_jdbc_columns.py)")
    return [name]


def _select_outputs(func_name):
    """Ordered output column names of every df.select(...) in the function."""
    outputs = []
    for call in ast.walk(_read_function("pg_sink.py", func_name)):
        if (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
                and call.func.attr == "select"):
            for arg in call.args:
                outputs.extend(_require_name(arg, func_name))
    if not outputs:
        raise AssertionError(f"pg_sink.{func_name}: no df.select(...) found")
    return outputs


def _select_sources(func_name):
    """Source column name feeding each select item (unresolvable -> None)."""
    sources = []
    for call in ast.walk(_read_function("pg_sink.py", func_name)):
        if not (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
                and call.func.attr == "select"):
            continue
        for arg in call.args:
            starred = _starred_names(arg, func_name)
            if starred is not None:
                # registry keys are Silver columns by construction (pinned
                # by MetricDimensionContractTests.test_silver_schema_carries_all_metrics)
                sources.extend(starred)
                continue
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
    """Field names of silver_schema() in spark/jobs/common.py (static parse).

    The field list is deliberately explicit (no loop over METRIC_NAMES) so
    this guard can pin the JDBC write contract statically.
    """
    fields = []
    for node in ast.walk(_read_function("common.py", "silver_schema")):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == "StructField" and node.args
                and _string_constant(node.args[0]) is not None):
            fields.append(_string_constant(node.args[0]))
    if not fields:
        raise AssertionError("no StructField(...) calls found in silver_schema()")
    return fields


def _metric_config_keys():
    """Metric names of METRIC_CONFIG in common.py (static parse of the dict)."""
    tree = ast.parse((JOBS_DIR / "common.py").read_text(encoding="utf-8"))
    for node in tree.body:
        if (isinstance(node, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == "METRIC_CONFIG"
                        for t in node.targets)):
            keys = [_string_constant(k) for k in node.value.keys]
            if None in keys:
                raise AssertionError("METRIC_CONFIG keys must be string literals")
            return keys
    raise AssertionError("METRIC_CONFIG not found in common.py")


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
                f"JDBC select source {source!r} is not a silver_schema() column")


class MetricDimensionContractTests(unittest.TestCase):
    """The metric registry drives every layer; this pins them together."""

    def test_staging_metrics_follow_metric_config(self):
        expected = _schema_columns("staging_stream_events")
        metrics = _metric_config_keys()
        for name in metrics:
            self.assertIn(name, expected,
                          f"staging_stream_events misses metric {name!r}")
        # identity + metric + provenance layout: everything else is fixed
        non_metrics = [c for c in expected if c not in metrics]
        self.assertEqual(
            non_metrics,
            ["event_id", "sensor_id", "event_time", "received_at",
             "kafka_topic", "kafka_partition", "kafka_offset"])

    def test_hourly_staging_carries_metric_dimension(self):
        self.assertIn("metric", _schema_columns("staging_hourly_agg"))
        self.assertIn("metric", _schema_columns("gold.sensor_hourly"))

    def test_silver_schema_carries_all_metrics(self):
        silver = _silver_schema_fields()
        for name in _metric_config_keys():
            self.assertIn(name, silver,
                          f"silver_schema() misses metric {name!r}")
        self.assertIn("metric_issues", silver,
                      "silver_schema() must carry the per-metric reason column")

    def test_sensor_latest_carries_all_metrics(self):
        columns = _schema_columns("sensor_latest")
        for name in _metric_config_keys():
            self.assertIn(name, columns,
                          f"sensor_latest misses metric {name!r}")

    def test_alerts_table_carries_rule_attributes(self):
        columns = _schema_columns("alerts")
        for name in ("event_id", "rule_id", "sensor_id", "event_time", "metric",
                     "threshold", "direction", "value", "temperature_c",
                     "created_at"):
            self.assertIn(name, columns, f"alerts misses column {name!r}")


if __name__ == "__main__":
    unittest.main()
