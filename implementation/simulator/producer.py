#!/usr/bin/env python3
"""Environmental sensor simulator publishing the canonical IoT contract over MQTT.

Topic layout:  <topic_prefix>/<sensor_id>/telemetry   (default sensors/zoneA_WEATHER_01/telemetry)
QoS:           1 (at-least-once), each publish waits for PUBACK (bounded wait).

Identifier contract (plan E3.1, multi-source demo):
    sensor_id = {prefix}_{TYPE}_{NNN}     e.g. zoneA_WEATHER_03
    event_id  = {prefix}-{seq:06d}        e.g. zoneA-000042
Station TYPE cycles WEATHER -> AIR -> MULTI over the sensor number:
    WEATHER: temperature_c, humidity_pct, pressure_hpa, light_lux
    AIR:     co2_ppm, pm25_ugm3
    MULTI:   all six metrics
A payload is a data-fusion VECTOR: it contains only the metrics of its station
type; absent metrics are simply not sent (the pipeline treats them as NULL).

Canonical payload example (MULTI station):
    {
      "event_id":      "zoneA-000042",
      "sensor_id":     "zoneA_MULTI_03",
      "event_time":    "2026-09-18T10:00:00+07:00",
      "temperature_c": 36.2,
      "humidity_pct":  61.5,
      "co2_ppm":       612.0,
      "pressure_hpa":  1008.4,
      "pm25_ugm3":     18.6,
      "light_lux":     31200.0,
      "ingest_time":   "2026-09-18T03:00:00.120Z"
    }
Optional legacy fields kept for compatibility: sensor_type, unit, location,
sequence_no; `value` remains a legacy alias of `temperature_c` (documented in
README).

Fault-injection profile (--fault-inject, seeded => deterministic; the faulted
metric is drawn from the metrics present in the event):
    2.0% duplicate event (re-send the previous event verbatim)
    1.0% missing field   (drop one metric from the payload)
    1.0% non-numeric metric (metric = "hot")
    1.0% invalid timestamp (event_time = "not-a-timestamp")
    1.0% out-of-physical-bounds (metric = 10x its upper bound)
    0.5% late 30s  (valid, must stay in Silver)
    0.5% late 5min (valid, must stay in Silver)

Usage examples:
    python producer.py --rate 5 --duration 60
    python producer.py --events 200 --rate 20 --fault-inject --seed 42
    python producer.py --rate 10            # runs forever until SIGTERM

Environment overrides (used by compose): MQTT_BROKER, MQTT_PORT, TOPIC_PREFIX,
RATE, SENSORS, SENSOR_PREFIX, FAULT_INJECT, SPIKE_PCT, MANIFEST_FILE.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt

# Station types and the metrics each one publishes (subset of METRIC_CONFIG
# semantics mirrored from spark/jobs/common.py; kept literal here because the
# simulator image does not ship the Spark jobs).
STATION_METRICS = {
    "WEATHER": ("temperature_c", "humidity_pct", "pressure_hpa", "light_lux"),
    "AIR": ("co2_ppm", "pm25_ugm3"),
    "MULTI": ("temperature_c", "humidity_pct", "co2_ppm", "pressure_hpa",
              "pm25_ugm3", "light_lux"),
}
STATION_CYCLE = ("WEATHER", "AIR", "MULTI")

# Per-metric generator profile: normal band (mean, spread) plus an optional
# alert-crossing spike band (lo, hi) used by --spike-pct so every alertable
# metric demonstrably crosses its threshold. Values stay well inside the
# physical bounds of spark/jobs/common.py METRIC_CONFIG.
METRIC_PROFILE = {
    "temperature_c": (30.0, 5.0, (35.5, 42.0)),
    "humidity_pct": (60.0, 10.0, (82.0, 95.0)),
    "co2_ppm": (600.0, 150.0, (1100.0, 1800.0)),
    "pressure_hpa": (1010.0, 10.0, (930.0, 945.0)),  # low-direction alerts
    "pm25_ugm3": (20.0, 8.0, (40.0, 80.0)),
    "light_lux": (30000.0, 15000.0, None),
}

# Physical upper bounds (only used by the out-of-bounds fault injection).
METRIC_UPPER_BOUND = {
    "temperature_c": 200.0, "humidity_pct": 100.0, "co2_ppm": 5000.0,
    "pressure_hpa": 1100.0, "pm25_ugm3": 1000.0, "light_lux": 200000.0,
}

LOCATIONS = [f"zone-{i:02d}" for i in range(1, 9)]


def iso_utc(dt: datetime) -> str:
    """ISO-8601 UTC with milliseconds and trailing Z."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def station_type(sensor_no: int) -> str:
    """Deterministic station type for a sensor number (cycles the 3 types)."""
    return STATION_CYCLE[(sensor_no - 1) % len(STATION_CYCLE)]


def _metric_value(name: str, spike: bool) -> float:
    """One reading for a metric; spike=True uses the alert-crossing band."""
    mean, spread, spike_band = METRIC_PROFILE[name]
    if spike and spike_band is not None:
        lo, hi = spike_band
        return round(random.uniform(lo, hi), 2)
    return round(mean + random.uniform(-spread, spread), 2)


def make_event(seq: int, sensors: int, prefix: str, spike_pct: float,
               event_time: datetime | None = None) -> dict:
    """Generate one clean event following the canonical contract.

    With probability ``spike_pct`` one random alertable metric of this
    station's vector gets an alert-crossing reading (so temperature, humidity,
    CO2, pressure and PM2.5 alerts all occur in the demo).
    """
    now = datetime.now(timezone.utc)
    evt_time = event_time or now
    sensor_no = (seq - 1) % sensors + 1
    stype = station_type(sensor_no)
    present = STATION_METRICS[stype]

    spike = random.random() * 100.0 < spike_pct
    event = {
        "event_id": f"{prefix}-{seq:06d}",
        "sensor_id": f"{prefix}_{stype}_{sensor_no:02d}",
        "event_time": evt_time.isoformat(),
        "ingest_time": iso_utc(now),
    }
    if present:
        spike_metric = random.choice(present) if spike else None
        for name in present:
            event[name] = _metric_value(name, spike and name == spike_metric)
    # optional legacy fields (documented as optional in the contract)
    event.update({
        "sensor_type": stype.lower(),
        "location": random.choice(LOCATIONS),
        "sequence_no": seq,
    })
    return event


def apply_fault(event: dict, fault: str) -> dict:
    """Mutate a fresh event according to one fault type.

    Metric faults pick one metric PRESENT in the payload (station subset),
    so per-metric quarantine/quality rates stay measurable per fault profile.
    """
    metrics = [k for k in event if k in METRIC_PROFILE]
    target = random.choice(metrics) if metrics else None
    if fault == "missing_field" and target:
        event.pop(target, None)
    elif fault == "non_numeric" and target:
        event[target] = "hot"
    elif fault == "bad_timestamp":
        event["event_time"] = "not-a-timestamp"
    elif fault == "out_of_bounds" and target:
        event[target] = round(METRIC_UPPER_BOUND[target] * 10, 2)
    elif fault == "late_30s":
        event["event_time"] = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
    elif fault == "late_5min":
        event["event_time"] = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    return event


def pick_fault() -> str | None:
    """Return the fault type for this tick, or None (deterministic under seed)."""
    r = random.random() * 100.0
    if r < 2.0:
        return "duplicate"
    if r < 3.0:
        return "missing_field"
    if r < 4.0:
        return "non_numeric"
    if r < 5.0:
        return "bad_timestamp"
    if r < 6.0:
        return "out_of_bounds"
    if r < 6.5:
        return "late_30s"
    if r < 7.0:
        return "late_5min"
    return None


class RateLimiter:
    """Paced limiter: sleeps so the average rate matches the target."""

    def __init__(self, rate: float):
        self.interval = 1.0 / rate if rate > 0 else 0.0
        self.next_ts = time.monotonic()

    def wait(self) -> None:
        if self.interval <= 0:
            return
        now = time.monotonic()
        if now < self.next_ts:
            time.sleep(self.next_ts - now)
        # catch up but never accumulate more than 1s of backlog
        self.next_ts = max(self.next_ts + self.interval, now - 1.0)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--broker", default=os.environ.get("MQTT_BROKER", "localhost"))
    p.add_argument("--port", type=int, default=int(os.environ.get("MQTT_PORT", "1883")))
    p.add_argument("--topic-prefix", default=os.environ.get("TOPIC_PREFIX", "sensors"))
    p.add_argument("--rate", type=float, default=float(os.environ.get("RATE", "5")),
                   help="events per second (0 = unlimited)")
    p.add_argument("--duration", type=float, default=float(os.environ.get("DURATION", "0")),
                   help="seconds to run; 0 = forever")
    p.add_argument("--events", type=int, default=int(os.environ.get("EVENTS", "0")),
                   help="total events to send; 0 = unlimited")
    p.add_argument("--sensors", type=int, default=int(os.environ.get("SENSORS", "5")))
    p.add_argument("--prefix", default=os.environ.get("SENSOR_PREFIX", "sim01"))
    p.add_argument("--spike-pct", type=float, default=float(os.environ.get("SPIKE_PCT", "2")))
    p.add_argument("--fault-inject", action=argparse.BooleanOptionalAction,
                   default=os.environ.get("FAULT_INJECT", "0") == "1")
    p.add_argument("--seed", type=int, default=None,
                   help="seed RNG for reproducible manifests/tests")
    p.add_argument("--manifest", default=os.environ.get(
        "MANIFEST_FILE", "/data/bigdata/logs/simulator-manifest.jsonl"),
        help="JSONL manifest path (set to empty string to disable)")
    args = p.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    manifest_fh = None
    if args.manifest:
        os.makedirs(os.path.dirname(args.manifest), exist_ok=True)
        # append mode: multiple runs extend the manifest; each line carries run info
        manifest_fh = open(args.manifest, "a", encoding="utf-8", buffering=1)

    def manifest(record: dict) -> None:
        if manifest_fh:
            manifest_fh.write(json.dumps(record, separators=(",", ":")) + "\n")

    # client id derives from the prefix: parallel simulators (run-multi-sim.sh)
    # must not kick each other's MQTT session (unique client ids required).
    client_id = f"iot-sim-{args.prefix}"
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                         client_id=client_id,
                         protocol=mqtt.MQTTv311)
    client.connect(args.broker, args.port, keepalive=30)
    client.loop_start()

    running = {"flag": True}

    def _stop(signum, frame):  # noqa: ARG001
        running["flag"] = False

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    limiter = RateLimiter(args.rate)
    last_event: dict | None = None

    seq = 0
    sent = 0
    ack_fail = 0
    faults = 0
    duplicates = 0
    start = time.monotonic()
    last_report = start

    while running["flag"]:
        if args.events and sent >= args.events:
            break
        if args.duration and (time.monotonic() - start) >= args.duration:
            break
        limiter.wait()

        fault = pick_fault() if args.fault_inject else None
        if fault == "duplicate" and last_event is not None:
            event = dict(last_event)
            duplicates += 1
        else:
            seq += 1
            event = make_event(seq, args.sensors, args.prefix, args.spike_pct)
            if fault:
                event = apply_fault(event, fault)
            if fault:
                faults += 1
            last_event = event

        topic = f"{args.topic_prefix}/{event['sensor_id']}/telemetry"
        payload = json.dumps(event, separators=(",", ":"))
        info = client.publish(topic, payload, qos=1)
        try:
            info.wait_for_publish(timeout=5.0)
            # is_published() reflects PUBACK arrival for QoS1
            published = bool(info.is_published())
        except (ValueError, RuntimeError):
            published = False
        if not published:
            ack_fail += 1
            print(f"[simulator] PUBACK timeout/error event_id={event.get('event_id')} "
                  f"topic={topic}", flush=True)

        manifest({
            "ts": iso_utc(datetime.now(timezone.utc)),
            "event_id": event.get("event_id"),
            "sensor_id": event.get("sensor_id"),
            "topic": topic,
            "qos": 1,
            "published": published,
            "fault": fault,
            "event_time": event.get("event_time"),
            "metrics": {k: event[k] for k in METRIC_PROFILE if k in event},
        })
        sent += 1

        now = time.monotonic()
        if now - last_report >= 10.0:
            elapsed = now - start
            print(f"[simulator] sent={sent} rate={sent / elapsed:.1f} evt/s "
                  f"(faults={faults} dups={duplicates} puback_fail={ack_fail})", flush=True)
            last_report = now

    client.loop_stop()
    client.disconnect()
    if manifest_fh:
        manifest_fh.close()
    elapsed = time.monotonic() - start
    print(f"[simulator] DONE sent={sent} in {elapsed:.1f}s "
          f"(faults={faults} duplicates={duplicates} puback_fail={ack_fail})", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
