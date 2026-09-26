"""Shared validation rules for the IoT telemetry contract (Q2a/Q2b and batch).

Two synchronized implementations live in this module:

  * ``classify_event`` / ``parse_iso_utc`` / ``resolve_metric`` /
    ``evaluate_metrics`` — a pure-Python reference that runs WITHOUT PySpark;
    unit tests (implementation/tests/test_validation.py) exercise it directly.
  * ``build_validity_columns`` — the PySpark expression builder used by the
    streaming queries (Q2a Silver / Q2b Quarantine). Semantically identical
    to the Python reference; both must be updated together.

Two-level error model (plan E3.2/E3.3, user decision 2026-09-18):

  1. IDENTITY errors => the WHOLE row goes to Quarantine with ``error_reason``
     from the stable vocabulary:
         missing_or_invalid_event_time
         missing_or_invalid_ingest_time
         missing_field:sensor_id
         missing_field:event_id
     (naive timestamps are interpreted as UTC; unparseable ones quarantine).

  2. METRIC issues (per metric, from METRIC_CONFIG in common.py) do NOT
     quarantine the row: the row still enters Silver with that metric masked
     to NULL and a reason recorded in ``metric_issues`` (";"-joined):
         non_numeric:<metric>            (present but not a number)
         value_out_of_bounds:<metric>    (outside the inclusive physical bounds)
     An ABSENT metric (station subset) is normal — no reason, no flag.

Notes:
  * late-but-valid events are NEVER quarantined; they only get the
    informational ``is_late`` flag in Silver (plan 3.2);
  * a metric value beyond its alert threshold is still Silver — alerts are a
    downstream Q3 rule driven by the same METRIC_CONFIG table (pg_sink.py);
  * adding a metric = one METRIC_CONFIG row; no validation/alert code change.
"""

import os
from datetime import datetime, timezone

from common import METRIC_ALIASES, METRIC_CONFIG, METRIC_NAMES

# Informational late-event flag threshold (seconds), env-tunable.
LATE_THRESHOLD_S = float(os.environ.get("LATE_THRESHOLD_S", "60"))

# Identity-level quarantine reasons (stable contract, docs/ARCHITECTURE.md).
R_MISSING_EVENT_TIME = "missing_or_invalid_event_time"
R_MISSING_INGEST_TIME = "missing_or_invalid_ingest_time"
R_MISSING_SENSOR = "missing_field:sensor_id"
R_MISSING_EVENT_ID = "missing_field:event_id"

# Per-metric reason templates (composed as template.format(metric=<name>)).
R_NON_NUMERIC = "non_numeric:{metric}"
R_OUT_OF_BOUNDS = "value_out_of_bounds:{metric}"


# ------------------------------------------------------- pure python side ----

def parse_iso_utc(s):
    """Parse an ISO-8601 timestamp string; return an aware UTC datetime or None.

    Accepts 'Z' suffix, explicit offsets like '+07:00' and fractional seconds
    (Python >= 3.11 ``fromisoformat``). Empty/whitespace/non-string -> None.
    Naive strings (no offset) are interpreted as UTC — mirrored in Spark by
    session timezone UTC.
    """
    if not isinstance(s, str) or not s.strip():
        return None
    try:
        dt = datetime.fromisoformat(s.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _clean_str(v):
    """Mirror of SQL trim(): strip strings, pass None through, else str()."""
    if v is None:
        return None
    if isinstance(v, str):
        v = v.strip()
        return v if v else None
    return str(v)


def resolve_metric(d, name):
    """Return (value_or_None, present_bool) for one metric field.

    Mirrors the Spark expression ``cast(coalesce(nullif(trim(<aliases>),'')) AS
    double)``: the canonical field is tried first, then any legacy alias from
    METRIC_ALIASES (temperature_c falls back to ``value``). An empty canonical
    field falls through to the alias; a present-but-unparseable value returns
    (None, True).
    """
    raw = _clean_str(d.get(name))
    if raw is None:
        for alias in METRIC_ALIASES.get(name, ()):
            raw = _clean_str(d.get(alias))
            if raw is not None:
                break
    if raw is None:
        return None, False
    try:
        return float(raw), True
    except (TypeError, ValueError):
        return None, True


def evaluate_metrics(d):
    """Per-metric evaluation (pure-Python mirror of build_validity_columns).

    Returns (values, issues):
      * values: dict metric -> float | None; a metric absent from the payload
        (station subset) maps to None with NO issue;
      * issues: list of reason strings ("non_numeric:<m>" or
        "value_out_of_bounds:<m>") in METRIC_NAMES order; a metric with an
        issue is always None in ``values`` (masked, not dropped).
    """
    values, issues = {}, []
    for name in METRIC_NAMES:
        lo, hi = METRIC_CONFIG[name][0], METRIC_CONFIG[name][1]
        value, present = resolve_metric(d, name)
        if not present:
            values[name] = None
            continue
        if value is None:
            issues.append(R_NON_NUMERIC.format(metric=name))
            values[name] = None
        elif value < lo or value > hi:
            issues.append(R_OUT_OF_BOUNDS.format(metric=name))
            values[name] = None
        else:
            values[name] = value
    return values, issues


def classify_event(d):
    """Identity-level classification (pure-Python mirror).

    Returns (is_valid, error_reason). Check order MUST stay identical to
    ``build_validity_columns``. Metric problems do NOT participate here —
    they never quarantine the row (see evaluate_metrics).
    """
    if parse_iso_utc(d.get("event_time")) is None:
        return False, R_MISSING_EVENT_TIME
    if parse_iso_utc(d.get("ingest_time")) is None:
        return False, R_MISSING_INGEST_TIME
    if _clean_str(d.get("sensor_id")) is None:
        return False, R_MISSING_SENSOR
    if _clean_str(d.get("event_id")) is None:
        return False, R_MISSING_EVENT_ID
    return True, None


# --------------------------------------------------------- pyspark side ----

def payload_schema():
    """All-string schema for from_json so type errors land in the per-metric
    issue path (Bronze stays lossless, nothing breaks the parse)."""
    from pyspark.sql.types import StringType, StructField, StructType

    fields = ["event_id", "sensor_id", "event_time", "ingest_time",
              "sensor_type", "unit", "value", "location", "sequence_no"]
    fields.extend(n for n in METRIC_NAMES if n not in fields)
    return StructType([StructField(f, StringType(), True) for f in fields])


def build_validity_columns(df):
    """Attach parsed + normalized + classification columns to a Bronze df.

    Input columns: raw_payload, mqtt_topic, kafka_topic, kafka_partition,
    kafka_offset, kafka_timestamp, received_at_utc.

    Output adds: event_id, sensor_id, event_time_utc, event_time_src,
    <6 metric columns> (masked per METRIC_CONFIG), metric_issues,
    ingest_time_utc, ingest_time_src, sensor_type, unit, location,
    sequence_no, is_late, is_valid, error_reason, record_key.
    """
    from pyspark.sql import functions as F

    df = df.withColumn("parsed", F.from_json(F.col("raw_payload"), payload_schema()))
    p = F.col("parsed")

    # Cast semantics under ANSI-off: unparseable -> NULL (identity => quarantine).
    event_time_utc = p["event_time"].cast("timestamp")
    ingest_time_utc = p["ingest_time"].cast("timestamp")

    # Identity check order mirrors classify_event() exactly.
    error_reason = (
        F.when(event_time_utc.isNull(), F.lit(R_MISSING_EVENT_TIME))
        .when(ingest_time_utc.isNull(), F.lit(R_MISSING_INGEST_TIME))
        .when(p["sensor_id"].isNull() | (F.trim(p["sensor_id"]) == F.lit("")),
              F.lit(R_MISSING_SENSOR))
        .when(p["event_id"].isNull() | (F.trim(p["event_id"]) == F.lit("")),
              F.lit(R_MISSING_EVENT_ID))
    )

    # Per-metric masking: absent -> NULL (no issue); present-but-unparseable
    # or out-of-bounds -> NULL + reason; in-bounds -> value. Table-driven from
    # METRIC_CONFIG/METRIC_ALIASES: adding a metric needs no edit here.
    metric_cols = []
    issue_cols = []
    for name in METRIC_NAMES:
        lo, hi = METRIC_CONFIG[name][0], METRIC_CONFIG[name][1]
        raw_candidates = (name,) + METRIC_ALIASES.get(name, ())
        raw = F.coalesce(*[
            F.nullif(F.trim(p[alias]), F.lit("")) for alias in raw_candidates
        ])
        value = raw.cast("double")
        masked = (F.when(raw.isNull(), F.lit(None).cast("double"))
                  .when(value.isNull(), F.lit(None).cast("double"))
                  .when((value < F.lit(lo)) | (value > F.lit(hi)),
                        F.lit(None).cast("double"))
                  .otherwise(value))
        metric_cols.append(masked.alias(name))
        issue_cols.append(
            F.when(raw.isNotNull() & value.isNull(),
                   F.lit(R_NON_NUMERIC.format(metric=name)))
            .when(value.isNotNull()
                  & ((value < F.lit(lo)) | (value > F.lit(hi))),
                   F.lit(R_OUT_OF_BOUNDS.format(metric=name)))
        )

    # concat_ws skips NULLs; an all-NULL input yields "" -> normalize to NULL.
    joined_issues = F.concat_ws(";", *issue_cols)
    metric_issues = F.when(F.length(joined_issues) == 0, F.lit(None).cast("string")) \
                     .otherwise(joined_issues)

    late_seconds = (F.unix_timestamp(F.col("received_at_utc"))
                    - F.unix_timestamp(event_time_utc))
    is_late = F.coalesce(late_seconds > F.lit(LATE_THRESHOLD_S), F.lit(False))

    record_key = F.coalesce(
        p["event_id"],
        F.concat_ws("-", F.col("kafka_topic"),
                    F.col("kafka_partition").cast("string"),
                    F.col("kafka_offset").cast("string")),
    )

    out = df.select(
        "raw_payload", "mqtt_topic", "kafka_topic", "kafka_partition",
        "kafka_offset", "kafka_timestamp", "received_at_utc",
        p["event_id"].alias("event_id"),
        p["sensor_id"].alias("sensor_id"),
        event_time_utc.alias("event_time_utc"),
        p["event_time"].alias("event_time_src"),
        *metric_cols,
        metric_issues.alias("metric_issues"),
        ingest_time_utc.alias("ingest_time_utc"),
        p["ingest_time"].alias("ingest_time_src"),
        p["sensor_type"].alias("sensor_type"),
        p["unit"].alias("unit"),
        p["location"].alias("location"),
        p["sequence_no"].alias("sequence_no"),
        is_late.alias("is_late"),
        error_reason.isNull().alias("is_valid"),
        error_reason.alias("error_reason"),
        record_key.alias("record_key"),
    )
    return out
