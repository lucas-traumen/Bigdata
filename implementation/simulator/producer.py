#!/usr/bin/env python3
"""Sensor event simulator for the Big Data IoT platform.

Produces sensor events (JSON, 9-field contract from the report) to Kafka
topic `sensor.raw` using confluent-kafka.

Fault-injection profile (when --fault-inject is set):
  - 2.0%  duplicate event_id (re-send a recently produced event verbatim)
  - 1.0%  missing field (drop `value`)
  - 1.0%  value = 9999 (out of physical bounds)
  - 0.5%  late event: event_time = now - 30s
  - 0.5%  late event: event_time = now - 5min

Usage examples:
  python producer.py --rate 100 --duration 100        # 100 evt/s for 100s
  python producer.py --events 10000 --rate 500        # exactly 10000 events
  python producer.py --rate 200 --fault-inject        # with fault profile

Environment overrides (used by docker-compose): BOOTSTRAP_SERVERS, TOPIC,
RATE, DURATION, EVENTS, FAULT_INJECT.
"""

import argparse
import json
import os
import random
import signal
import sys
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

LOCATIONS = [f"zone-{i:02d}" for i in range(1, 9)]
SENSOR_TYPES = ["temperature", "humidity", "pressure", "vibration"]
UNITS = {"temperature": "C", "humidity": "%", "pressure": "kPa", "vibration": "mm/s"}

# Value generator: mean per sensor type, plus drift (sinusoid + noise).
# NOTE: pressure is in kPa (~101.3 kPa = 1 atm) so ALL clean values stay inside
# the Silver physical bounds [-50, 200] used by the pipeline (report: value
# must satisfy -50 <= value <= 200). hPa (~1013) would be out of bounds.
BASE_MEAN = {"temperature": 30.0, "humidity": 60.0, "pressure": 101.3, "vibration": 2.0}
BASE_SPREAD = {"temperature": 5.0, "humidity": 10.0, "pressure": 0.5, "vibration": 0.8}

RANDOM = random.Random(20260827)  # deterministic by default for reproducibility


def iso_utc(dt: datetime) -> str:
    """ISO-8601 with milliseconds and trailing Z, e.g. 2026-08-24T10:15:32.120Z."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def make_event(seq: int, event_time: datetime | None = None,
               sensor_id: str | None = None,
               sensor_type: str | None = None) -> dict:
    """Generate one clean sensor event (8 fields of the contract)."""
    st = sensor_type or RANDOM.choice(SENSOR_TYPES)
    mean = BASE_MEAN[st]
    spread = BASE_SPREAD[st]
    value = round(mean + RANDOM.uniform(-spread, spread)
                  + spread * 0.3 * (1 + RANDOM.random()), 2)
    now = datetime.now(timezone.utc)
    evt_time = event_time or now
    sensor = sensor_id or f"sensor-{RANDOM.randint(1, 500):06d}"
    return {
        "event_id": str(uuid.uuid4()),
        "sensor_id": sensor,
        "event_time": iso_utc(evt_time),
        "ingest_time": iso_utc(now),
        "sensor_type": st,
        "unit": UNITS[st],
        "value": value,
        "location": RANDOM.choice(LOCATIONS),
        "sequence_no": seq,
    }


def apply_fault(event: dict, fault_type: str) -> dict:
    """Apply one fault to a fresh event (returns possibly-mutated copy)."""
    if fault_type == "missing_field":
        event.pop("value", None)
    elif fault_type == "out_of_bounds":
        event["value"] = 9999
    elif fault_type == "late_30s":
        event["event_time"] = iso_utc(
            datetime.now(timezone.utc) - timedelta(seconds=30))
    elif fault_type == "late_5min":
        event["event_time"] = iso_utc(
            datetime.now(timezone.utc) - timedelta(minutes=5))
    return event


def pick_fault() -> str | None:
    """Return fault type according to the profile probabilities, or None."""
    r = RANDOM.random()
    if r < 0.02:
        return "duplicate"
    if r < 0.03:
        return "missing_field"
    if r < 0.04:
        return "out_of_bounds"
    if r < 0.045:
        return "late_30s"
    if r < 0.05:
        return "late_5min"
    return None


class RateLimiter:
    """Simple paced limiter: sleeps so average rate matches target."""

    def __init__(self, rate: float):
        self.interval = 1.0 / rate if rate > 0 else 0.0
        self.next_ts = time.monotonic()

    def wait(self) -> None:
        if self.interval <= 0:
            return
        now = time.monotonic()
        if now < self.next_ts:
            time.sleep(self.next_ts - now)
        # Allow catching up but never more than 1 second of backlog.
        self.next_ts = max(self.next_ts + self.interval, now - 1.0)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bootstrap", default=os.environ.get("BOOTSTRAP_SERVERS", "kafka-1:9092"))
    p.add_argument("--topic", default=os.environ.get("TOPIC", "sensor.raw"))
    p.add_argument("--rate", type=float, default=float(os.environ.get("RATE", "100")),
                   help="events per second (0 = unlimited)")
    p.add_argument("--duration", type=float, default=float(os.environ.get("DURATION", "0")),
                   help="seconds to run; 0 = forever")
    p.add_argument("--events", type=int, default=int(os.environ.get("EVENTS", "0")),
                   help="total events to send; 0 = unlimited")
    p.add_argument("--fault-inject", action=argparse.BooleanOptionalAction,
                   default=os.environ.get("FAULT_INJECT", "0") == "1")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--fast-forward", type=int,
                   default=int(os.environ.get("FAST_FORWARD", "0")),
                   help="after the main batch, send 10 extra events with "
                        "event_time = now + N seconds to push event-time "
                        "watermarks forward (used by smoke tests to close "
                        "Gold windows without waiting 10+ real minutes)")
    args = p.parse_args()

    if args.seed is not None:
        RANDOM.seed(args.seed)

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "linger.ms": 20,           # small batches, low latency
        "compression.type": "lz4",
        "acks": "all",
        "client.id": "sensor-simulator",
    })

    running = {"flag": True}

    def _stop(signum, frame):  # noqa: ARG001
        running["flag"] = False

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    limiter = RateLimiter(args.rate)
    recent: deque[dict] = deque(maxlen=256)  # for duplicate injection

    seq = 0
    sent = 0
    duplicates = 0
    faults = 0
    start = time.monotonic()
    last_report = start

    while running["flag"]:
        if args.events and sent >= args.events:
            break
        if args.duration and (time.monotonic() - start) >= args.duration:
            break
        limiter.wait()

        fault = pick_fault() if args.fault_inject else None
        if fault == "duplicate" and recent:
            event = dict(RANDOM.choice(recent))   # re-send same event_id
            duplicates += 1
        else:
            seq += 1
            event = make_event(seq)
            if fault:
                event = apply_fault(event, fault)
                faults += 1
            recent.append(event)

        producer.produce(args.topic, json.dumps(event, separators=(",", ":")))
        sent += 1

        now = time.monotonic()
        if now - last_report >= 10.0:
            elapsed = now - start
            print(f"[simulator] sent={sent} rate={sent / elapsed:.1f} evt/s "
                  f"(faults={faults} dups={duplicates})", flush=True)
            last_report = now

    # Optional fast-forward: push a few events stamped in the (near) future
    # relative to the batch we just sent, so event-time watermarks advance and
    # windowed aggregations close without waiting real time.
    if args.fast_forward > 0:
        future = datetime.now(timezone.utc) + timedelta(seconds=args.fast_forward)
        for _ in range(10):
            seq += 1
            event = make_event(seq, event_time=future)
            producer.produce(args.topic, json.dumps(event, separators=(",", ":")))
            sent += 1
        print(f"[simulator] fast-forwarded watermark by +{args.fast_forward}s",
              flush=True)

    producer.flush(timeout=30)
    elapsed = time.monotonic() - start
    print(f"[simulator] DONE sent={sent} in {elapsed:.1f}s "
          f"(faults={faults} duplicates={duplicates})", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
