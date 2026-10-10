"""FastAPI serving layer for the IoT demo (single worker).

Read-only views over the PostgreSQL `app` database:
  GET /health                service + database status
  GET /api/sensors/latest    sensor_latest with pagination (and sensor filter)
  GET /api/alerts            alerts with pagination
  GET /api/stats             gold.sensor_hourly for a UTC [start,end) window;
                              auto-selects the stored Gold window when omitted
  GET /api/summary           compact serving + Gold snapshot for the dashboard
  GET /api/progress          internal pipeline progress counters

The pool is small (max 5) and connections are re-established with retry at
startup because the backend may start before PostgreSQL finishes initializing.
When the database is unreachable the endpoints answer 503; /health still
answers 200 with "degraded" so the container healthcheck reflects the process
being alive (the dashboard renders the error state itself).
"""

from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timedelta, timezone
from typing import Iterator

from fastapi import FastAPI, HTTPException, Query
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool

PG_SETTINGS = {
    "host": os.environ.get("PGHOST", "postgres"),
    "port": int(os.environ.get("PGPORT", "5432")),
    "dbname": os.environ.get("PGDATABASE", "app"),
    "user": os.environ.get("PGUSER", "bigdata"),
    "password": os.environ.get("PGPASSWORD", ""),
}

MAX_PAGE_LIMIT = 500
SUMMARY_CACHE_SECONDS = float(os.environ.get("SUMMARY_CACHE_SECONDS", "5"))
_summary_cache: dict[str, object] = {"expires": 0.0, "value": None}

pool: SimpleConnectionPool | None = None


def _init_pool_with_retry(tries: int = 60, delay_s: float = 2.0) -> None:
    global pool
    for attempt in range(1, tries + 1):
        try:
            pool = SimpleConnectionPool(1, 5, connect_timeout=3, **PG_SETTINGS)
            print("[backend] postgres connection pool ready", flush=True)
            return
        except Exception as exc:  # noqa: BLE001 - retry any connection error
            pool = None
            print(f"[backend] postgres not ready ({attempt}/{tries}): {exc}",
                  flush=True)
            time.sleep(delay_s)
    print("[backend] postgres unreachable; serving degraded until it returns",
          flush=True)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    _init_pool_with_retry()
    yield
    if pool is not None:
        pool.closeall()


app = FastAPI(title="IoT Big Data Demo API", version="1.0.0", lifespan=lifespan)


@contextmanager
def db_cursor() -> Iterator[RealDictCursor]:
    """Borrow a pooled connection; translate failures into HTTP 503."""
    if pool is None:
        raise HTTPException(status_code=503, detail="database unavailable")
    try:
        conn = pool.getconn()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}")
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
        conn.commit()
    except HTTPException:
        conn.rollback()
        raise
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        raise HTTPException(status_code=503, detail=f"database error: {exc}")
    finally:
        pool.putconn(conn)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_utc(s: str, param: str) -> datetime:
    try:
        dt = datetime.fromisoformat(s.strip().replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(status_code=400, detail=f"{param} is not ISO-8601: {s!r}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@app.get("/health")
def health():
    ok = False
    try:
        with db_cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
            ok = True
    except HTTPException:
        ok = False
    return {
        "status": "ok" if ok else "degraded",
        "database": ok,
        "checked_at": _now_iso(),
        "version": app.version,
    }


@app.get("/api/sensors/latest")
def sensors_latest(
    sensor_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1),
    offset: int = Query(default=0, ge=0),
):
    limit = min(limit, MAX_PAGE_LIMIT)
    with db_cursor() as cur:
        if sensor_id:
            cur.execute(
                "SELECT count(*) AS total FROM sensor_latest WHERE sensor_id = %s",
                (sensor_id,),
            )
            total = cur.fetchone()["total"]
            cur.execute(
                "SELECT * FROM sensor_latest WHERE sensor_id = %s ORDER BY sensor_id "
                "LIMIT %s OFFSET %s",
                (sensor_id, limit, offset),
            )
        else:
            cur.execute("SELECT count(*) AS total FROM sensor_latest")
            total = cur.fetchone()["total"]
            cur.execute(
                "SELECT * FROM sensor_latest ORDER BY sensor_id LIMIT %s OFFSET %s",
                (limit, offset),
            )
        rows = [dict(r) for r in cur.fetchall()]
    for r in rows:
        for k in ("event_time", "received_at", "updated_at"):
            if r.get(k) is not None:
                r[k] = r[k].isoformat()
    return {"items": rows, "total": total, "limit": limit, "offset": offset}


@app.get("/api/alerts")
def alerts(
    limit: int = Query(default=100, ge=1),
    offset: int = Query(default=0, ge=0),
):
    limit = min(limit, MAX_PAGE_LIMIT)
    with db_cursor() as cur:
        cur.execute("SELECT count(*) AS total FROM alerts")
        total = cur.fetchone()["total"]
        cur.execute(
            "SELECT * FROM alerts ORDER BY created_at DESC LIMIT %s OFFSET %s",
            (limit, offset),
        )
        rows = [dict(r) for r in cur.fetchall()]
    for r in rows:
        for k in ("event_time", "created_at"):
            if r.get(k) is not None:
                r[k] = r[k].isoformat()
    return {"items": rows, "total": total, "limit": limit, "offset": offset}


@app.get("/api/stats")
def stats(
    start: str | None = Query(default=None, description="UTC ISO-8601, inclusive"),
    end: str | None = Query(default=None, description="UTC ISO-8601, exclusive"),
    sensor_id: str | None = Query(default=None),
):
    """Read Gold stats and auto-select the stored demo window when unset."""
    auto_range = start is None and end is None
    available_start = None
    available_end = None
    with db_cursor() as cur:
        cur.execute(
            "SELECT min(hour_start) AS available_start, "
            "max(hour_start) AS last_hour FROM gold.sensor_hourly"
        )
        available = cur.fetchone()
        if available["available_start"] is not None:
            available_start = available["available_start"]
            available_end = available["last_hour"] + timedelta(hours=1)

        if auto_range and available_start is not None:
            start_dt = available_start
            end_dt = available_end
        else:
            end_dt = _parse_utc(end, "end") if end else datetime.now(timezone.utc)
            start_dt = _parse_utc(start, "start") if start else end_dt - timedelta(hours=24)

        if start_dt >= end_dt:
            raise HTTPException(status_code=400, detail="start must be before end")

        where = ["hour_start >= %s", "hour_start < %s"]
        params: list = [start_dt, end_dt]
        if sensor_id:
            where.append("sensor_id = %s")
            params.append(sensor_id)
        cur.execute(
            "SELECT sensor_id, hour_start, metric, event_count, avg_value, "
            "min_value, max_value, computed_at FROM gold.sensor_hourly "
            f"WHERE {' AND '.join(where)} ORDER BY hour_start, sensor_id, metric",
            params,
        )
        rows = [dict(r) for r in cur.fetchall()]
    for r in rows:
        for k in ("hour_start", "computed_at"):
            if r.get(k) is not None:
                r[k] = r[k].isoformat()
    return {
        "items": rows,
        "start": start_dt.isoformat(),
        "end": end_dt.isoformat(),
        "sensor_id": sensor_id,
        "auto_range": auto_range and available_start is not None,
        "available_start": available_start.isoformat() if available_start else None,
        "available_end": available_end.isoformat() if available_end else None,
    }


@app.get("/api/summary")
def summary():
    """Return a cached compact snapshot used by the demo dashboard header."""
    now = time.monotonic()
    cached = _summary_cache.get("value")
    if cached is not None and now < float(_summary_cache.get("expires", 0.0)):
        return cached
    with db_cursor() as cur:
        cur.execute(
            "SELECT count(*) AS events, "
            "count(*) FILTER (WHERE duplicate_count > 0) AS duplicated_events, "
            "coalesce(sum(duplicate_count), 0) AS duplicate_occurrences, "
            "max(first_processed_at) AS last_processed_at "
            "FROM processed_events"
        )
        serving = dict(cur.fetchone())
        cur.execute("SELECT count(*) AS sensors FROM sensor_latest")
        serving["sensors"] = cur.fetchone()["sensors"]
        cur.execute("SELECT count(*) AS alerts FROM alerts")
        serving["alerts"] = cur.fetchone()["alerts"]
        cur.execute(
            "SELECT count(*) AS gold_rows, "
            "coalesce(sum(event_count), 0) AS gold_readings, "
            "count(DISTINCT sensor_id) AS gold_sensors, "
            "count(DISTINCT metric) AS gold_metrics, "
            "count(DISTINCT hour_start) AS gold_hours, "
            "min(hour_start) AS gold_start, "
            "max(hour_start) AS gold_last_hour, "
            "max(computed_at) AS gold_computed_at "
            "FROM gold.sensor_hourly"
        )
        gold = dict(cur.fetchone())

    if serving.get("last_processed_at") is not None:
        serving["last_processed_at"] = serving["last_processed_at"].isoformat()
    if gold.get("gold_start") is not None:
        gold["gold_start"] = gold["gold_start"].isoformat()
        gold["gold_end"] = (
            datetime.fromisoformat(gold["gold_last_hour"].isoformat())
            + timedelta(hours=1)
        ).isoformat()
        gold["gold_last_hour"] = gold["gold_last_hour"].isoformat()
    else:
        gold["gold_end"] = None
        gold["gold_last_hour"] = None
    if gold.get("gold_computed_at") is not None:
        gold["gold_computed_at"] = gold["gold_computed_at"].isoformat()
    result = {**serving, **gold}
    _summary_cache["value"] = result
    _summary_cache["expires"] = now + SUMMARY_CACHE_SECONDS
    return result


@app.get("/api/progress")
def progress():
    """Internal counters to observe pipeline progress and measure latency."""
    with db_cursor() as cur:
        cur.execute(
            "SELECT count(*) AS events, "
            "count(*) FILTER (WHERE duplicate_count > 0) AS duplicates, "
            "max(first_processed_at) AS last_processed_at "
            "FROM processed_events"
        )
        row = dict(cur.fetchone())
        cur.execute("SELECT count(*) AS sensors FROM sensor_latest")
        row["sensors"] = cur.fetchone()["sensors"]
        cur.execute("SELECT count(*) AS alerts FROM alerts")
        row["alerts"] = cur.fetchone()["alerts"]
        cur.execute("SELECT count(*) AS sensor_hour_rows FROM gold.sensor_hourly")
        row["sensor_hour_rows"] = cur.fetchone()["sensor_hour_rows"]
    if row.get("last_processed_at") is not None:
        row["last_processed_at"] = row["last_processed_at"].isoformat()
    return row
