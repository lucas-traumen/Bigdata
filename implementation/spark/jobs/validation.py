"""Shared validation rules for the IoT telemetry contract (Q2a/Q2b and batch).

Two synchronized implementations live in this module:

  * ``classify_event`` / ``parse_iso_utc`` / ``resolve_temperature`` — a
    pure-Python reference that runs WITHOUT PySpark; unit tests
    (implementation/tests/test_validation.py) exercise it directly.
  * ``build_validity_columns`` — the PySpark expression builder used by the
    streaming queries (Q2a Silver / Q2b Quarantine). Semantically identical
    to the Python reference; both must be updated together.

Quarantine reason vocabulary (stable contract, see docs/ARCHITECTURE.md):
    missing_or_invalid_event_time
    missing_or_invalid_ingest_time
    missing_or_non_numeric_temperature   (absent, empty, or not numeric —
                                          including the legacy `value` alias)
    value_out_of_physical_bounds         (outside [BOUNDS_MIN, BOUNDS_MAX])
    missing_field:sensor_id
    missing_field:event_id

Notes:
  * late-but-valid events are NEVER quarantined; they only get the
    informational ``is_late`` flag in Silver (plan 3.2);
  * a valid temperature above the alert threshold (default 35 C) is still
    Silver — alerts are a downstream rule, not a validation rule;
  * naive timestamps (no offset) are interpreted as UTC on both sides.
"""

import os
from datetime import datetime, timezone

# Keep these in sync with implementation/.env.example defaults.
BOUNDS_MIN = float(os.environ.get("BOUNDS_MIN", "-50"))
BOUNDS_MAX = float(os.environ.get("BOUNDS_MAX", "200"))
ALERT_THRESHOLD_C = float(os.environ.get("ALERT_THRESHOLD_C", "35"))
LATE_THRESHOLD_S = float(os.environ.get("LATE_THRESHOLD_S", "60"))

REQUIRED_FIELDS = ("event_id", "sensor_id", "event_time", "temperature_c", "ingest_time")

R_MISSING_EVENT_TIME = "missing_or_invalid_event_time"
R_MISSING_INGEST_TIME = "missing_or_invalid_ingest_time"
R_BAD_TEMPERATURE = "missing_or_non_numeric_temperature"
R_OUT_OF_BOUNDS = "value_out_of_physical_bounds"
R_MISSING_SENSOR = "missing_field:sensor_id"
R_MISSING_EVENT_ID = "missing_field:event_id"


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


def resolve_temperature(d):
    """Return (value_or_None, present_bool) from temperature_c or legacy value.

    Mirrors the Spark expression ``cast(coalesce(nullif(trim(temperature_c),''),
    nullif(trim(value),'')) AS double)``: an empty temperature_c falls through
    to the legacy alias; a present-but-unparseable value returns (None, True).
    """
    raw = _clean_str(d.get("temperature_c"))
    if raw is None:
        raw = _clean_str(d.get("value"))  # documented legacy alias
    if raw is None:
        return None, False
    try:
        return float(raw), True
    except (TypeError, ValueError):
        return None, True


def classify_event(d):
    """Pure-Python mirror of the Spark classification.

    Returns (is_valid, error_reason). Check order MUST stay identical to
    ``build_validity_columns``.
    """
    if parse_iso_utc(d.get("event_time")) is None:
        return False, R_MISSING_EVENT_TIME
    if parse_iso_utc(d.get("ingest_time")) is None:
        return False, R_MISSING_INGEST_TIME
    if _clean_str(d.get("sensor_id")) is None:
        return False, R_MISSING_SENSOR
    if _clean_str(d.get("event_id")) is None:
        return False, R_MISSING_EVENT_ID
    value, present = resolve_temperature(d)
    if not present or value is None:
        return False, R_BAD_TEMPERATURE
    if value < BOUNDS_MIN or value > BOUNDS_MAX:
        return False, R_OUT_OF_BOUNDS
    return True, None


# --------------------------------------------------------- pyspark side ----

def payload_schema():
    """All-string schema for from_json so type errors land in quarantine
    instead of breaking the parse (Bronze stays lossless)."""
    from pyspark.sql.types import StringType, StructField, StructType

    fields = ["event_id", "sensor_id", "event_time", "temperature_c", "ingest_time",
              "sensor_type", "unit", "value", "location", "sequence_no"]
    return StructType([StructField(f, StringType(), True) for f in fields])


def build_validity_columns(df):
    """Attach parsed + normalized + classification columns to a Bronze df.

    Input columns: raw_payload, mqtt_topic, kafka_topic, kafka_partition,
    kafka_offset, kafka_timestamp, received_at_utc.

    Output adds: event_id, sensor_id, event_time_utc, event_time_src,
    temperature_c, ingest_time_utc, ingest_time_src, sensor_type, unit,
    location, sequence_no, is_late, is_valid, error_reason, record_key.
    """
    from pyspark.sql import functions as F

    df = df.withColumn("parsed", F.from_json(F.col("raw_payload"), payload_schema()))
    p = F.col("parsed")

    # Cast semantics under ANSI-off: unparseable -> NULL (=> quarantine).
    event_time_utc = p["event_time"].cast("timestamp")
    ingest_time_utc = p["ingest_time"].cast("timestamp")
    temp_raw = F.coalesce(
        F.nullif(F.trim(p["temperature_c"]), F.lit("")),
        F.nullif(F.trim(p["value"]), F.lit("")),
    )
    temperature_c = temp_raw.cast("double")

    late_seconds = (F.unix_timestamp(F.col("received_at_utc"))
                    - F.unix_timestamp(event_time_utc))
    is_late = F.coalesce(late_seconds > F.lit(LATE_THRESHOLD_S), F.lit(False))

    # Check order mirrors classify_event() exactly.
    error_reason = (
        F.when(event_time_utc.isNull(), F.lit(R_MISSING_EVENT_TIME))
        .when(ingest_time_utc.isNull(), F.lit(R_MISSING_INGEST_TIME))
        .when(p["sensor_id"].isNull() | (F.trim(p["sensor_id"]) == F.lit("")),
              F.lit(R_MISSING_SENSOR))
        .when(p["event_id"].isNull() | (F.trim(p["event_id"]) == F.lit("")),
              F.lit(R_MISSING_EVENT_ID))
        .when(temperature_c.isNull(), F.lit(R_BAD_TEMPERATURE))
        .when((temperature_c < F.lit(BOUNDS_MIN)) | (temperature_c > F.lit(BOUNDS_MAX)),
              F.lit(R_OUT_OF_BOUNDS))
    )

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
        temperature_c.alias("temperature_c"),
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
