#!/usr/bin/env python3
"""Sensor event simulator publishing the canonical IoT contract over MQTT.

Topic layout:  <topic_prefix>/<sensor_id>/telemetry   (default sensors/TEMP_01/telemetry)
QoS:           1 (at-least-once), each publish waits for PUBACK (bounded wait).

Canonical payload (5 required fields, plan 3.2):
    {
      "event_id":      "sim01-000001",
      "sensor_id":     "TEMP_01",
      "event_time":    "2026-09-13T10:00:00+07:00",
      "temperature_c": 36.2,
      "ingest_time":   "2026-09-13T03:00:00.120Z"
    }
Optional legacy fields kept for compatibility: sensor_type, unit, location,
sequence_no. `temperature_c` is the canonical value name; the pipeline treats
`value` as a legacy alias when `temperature_c` is absent (documented in README).

Fault-injection profile (--fault-inject, seeded => deterministic):
    2.0% duplicate event (re-send the previous event verbatim)
    1.0% missing field   (drop `temperature_c`)
    1.0% non-numeric temperature (temperature_c = "hot")
    1.0% invalid timestamp (event_time = "not-a-timestamp")
    1.0% out-of-physical-bounds (temperature_c = 9999)
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

LOCATIONS = [f"zone-{i:02d}" for i in range(1, 9)]

# Temperature generator profile: normal band around 30 C, spikes cross the
# 35 C alert threshold but stay far below the 200 C physical bound.
BASE_MEAN = 30.0
BASE_SPREAD = 5.0
SPIKE_LO, SPIKE_HI = 35.5, 42.0

def iso_utc(dt: datetime) -> str:
    """ISO-8601 UTC with milliseconds and trailing Z."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def make_event(seq: int, sensors: int, prefix: str, spike_pct: float,
               event_time: datetime | None = None) -> dict:
    """Generate one clean event following the canonical contract."""
    now = datetime.now(timezone.utc)
    evt_time = event_time or now
    sensor_no = (seq - 1) % sensors + 1
    if random.random() * 100.0 < spike_pct:
        value = round(random.uniform(SPIKE_LO, SPIKE_HI), 2)
    else:
        value = round(BASE_MEAN + random.uniform(-BASE_SPREAD, BASE_SPREAD), 2)
    return {
        "event_id": f"{prefix}-{seq:06d}",
        "sensor_id": f"TEMP_{sensor_no:02d}",
        "event_time": evt_time.isoformat(),
        "temperature_c": value,
        "ingest_time": iso_utc(now),
        # optional legacy fields (documented as optional in the contract)
        "sensor_type": "temperature",
        "unit": "C",
        "location": random.choice(LOCATIONS),
        "sequence_no": seq,
    }


def apply_fault(event: dict, fault: str) -> dict:
    """Mutate a fresh event according to one fault type."""
    if fault == "missing_field":
        event.pop("temperature_c", None)
    elif fault == "non_numeric":
        event["temperature_c"] = "hot"
    elif fault == "bad_timestamp":
        event["event_time"] = "not-a-timestamp"
    elif fault == "out_of_bounds":
        event["temperature_c"] = 9999
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
    p.add_argument("--sensors", type=int, default=int(os.environ.get("SENSORS", "20")))
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

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                         client_id="iot-simulator",
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
            "temperature_c": event.get("temperature_c"),
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
