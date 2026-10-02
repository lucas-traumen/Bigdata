#!/usr/bin/env python3
"""MQTT -> Kafka bridge for the IoT demo pipeline.

Contract:
  * subscribes  <MQTT_TOPIC_FILTER>  (default sensors/+/telemetry) at QoS 1
    with a persistent session (clean_session=False) and *manual* acknowledgement;
  * a bounded in-memory queue decouples the network thread from Kafka;
  * the MQTT PUBACK for a message is sent ONLY after Kafka reports a successful
    delivery (acks=all). If the bridge dies in between, the broker redelivers
    the unacknowledged message on reconnect (at-least-once, duplicates are
    deduplicated downstream by event_id) — this is NOT exactly-once;
  * every successful Kafka delivery is appended as one JSONL line to
    DELIVERY_LOG (event_id, kafka topic/partition/offset, delivered_at) so
    sent-vs-delivered gaps can be measured from the simulator manifest.

Loss windows (documented, by design of this demo):
  * MQTT queue overflow at the broker (max_queued_messages) drops messages;
  * overflow of the in-memory queue drops the message *without* acking, so the
    broker may redeliver it on reconnect (no ack => broker retains it only for
    persistent sessions);
  * messages published while the bridge is down are retained by the broker
    only if the session existed before (persistent session) and queue limits
    allow.
"""

from __future__ import annotations

import json
import math
import os
import queue
import signal
import sys
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from confluent_kafka import Producer


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Bridge:
    def __init__(self) -> None:
        self.mqtt_broker = os.environ.get("MQTT_BROKER", "localhost")
        self.mqtt_port = int(os.environ.get("MQTT_PORT", "1883"))
        self.topic_filter = os.environ.get("MQTT_TOPIC_FILTER", "sensors/+/telemetry")
        self.kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
        self.kafka_topic = os.environ.get("KAFKA_TOPIC", "sensor_raw")
        self.queue_size = int(os.environ.get("BRIDGE_QUEUE_SIZE", "5000"))
        try:
            self.poll_interval_s = float(os.environ.get("BRIDGE_POLL_INTERVAL_S", "0.01"))
        except ValueError as exc:
            raise ValueError("BRIDGE_POLL_INTERVAL_S must be in (0, 0.5] seconds") from exc
        if not math.isfinite(self.poll_interval_s) or not 0 < self.poll_interval_s <= 0.5:
            raise ValueError("BRIDGE_POLL_INTERVAL_S must be in (0, 0.5] seconds")
        self.delivery_log = os.environ.get("DELIVERY_LOG", "")

        self.work: "queue.Queue[tuple[mqtt.MQTTMessage, str | None]]" = queue.Queue(
            maxsize=self.queue_size)
        self.stopping = threading.Event()

        # paho >= 2.0 supports manual ack via the constructor kwarg; guarded
        # below so an older paho degrades to auto-ack with a loud warning.
        try:
            self.client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id="iot-bridge",
                clean_session=False,
                manual_ack=True,
                protocol=mqtt.MQTTv311,
            )
            self.manual_ack = True
        except TypeError:
            # older paho without manual_ack: fall back to auto-ack and log the
            # weaker delivery guarantee explicitly.
            self.client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id="iot-bridge",
                clean_session=False,
                protocol=mqtt.MQTTv311,
            )
            self.manual_ack = False
            print("[bridge] WARNING: paho manual_ack unavailable; MQTT ACK is sent "
                  "before Kafka delivery (weaker guarantee)", flush=True)

        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

        self.producer = Producer({
            "bootstrap.servers": self.kafka_bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 20,
            "compression.type": "lz4",
            "message.timeout.ms": 300000,
            "client.id": "iot-bridge",
        })

        self._log_fh = None
        if self.delivery_log:
            os.makedirs(os.path.dirname(self.delivery_log), exist_ok=True)
            self._log_fh = open(self.delivery_log, "a", encoding="utf-8", buffering=1)

        self.stats = {"received": 0, "dropped_queue_full": 0, "delivered": 0,
                      "delivery_errors": 0, "acked": 0,
                      "delivered_payload_bytes": 0}

    # ------------------------------------------------------------- logging --
    def _log(self, msg: str) -> None:
        print(f"[bridge] {msg}", flush=True)

    def _log_delivery(self, record: dict) -> None:
        if self._log_fh:
            self._log_fh.write(json.dumps(record, separators=(",", ":")) + "\n")

    # -------------------------------------------------------- paho callbacks --
    def _on_connect(self, client, userdata, flags, reason_code, properties=None):  # noqa: ARG002
        self._log(f"connected to MQTT {self.mqtt_broker}:{self.mqtt_port} "
                  f"rc={reason_code} session_present={getattr(flags, 'session_present', flags)}")
        client.subscribe(self.topic_filter, qos=1)

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties=None):  # noqa: ARG002
        self._log(f"disconnected from MQTT rc={reason_code} (broker will redeliver "
                  "unacked messages for the persistent session)")

    def _on_message(self, client, userdata, message):  # noqa: ARG002
        self.stats["received"] += 1
        event_id = self._extract_event_id(message.payload)
        try:
            self.work.put_nowait((message, event_id))
        except queue.Full:
            # Do NOT ack: the broker keeps the message in our persistent session
            # and redelivers after reconnect. If the queue stays saturated the
            # broker-side cap (max_queued_messages) eventually drops it.
            self.stats["dropped_queue_full"] += 1
            self._log(f"QUEUE_FULL drop (unacked, will redeliver on reconnect) "
                      f"event_id={event_id} mid={message.mid}")

    @staticmethod
    def _extract_event_id(payload: bytes) -> str | None:
        try:
            return json.loads(payload.decode("utf-8")).get("event_id")
        except Exception:  # malformed JSON is forwarded unchanged; id unknown
            return None

    # ------------------------------------------------------------ kafka side --
    def _delivery_report(self, err, msg, event_id: str | None, mqtt_mid: int,
                         mqtt_qos: int, payload_bytes: int) -> None:
        if err is not None:
            self.stats["delivery_errors"] += 1
            # no MQTT ack: broker redelivers on reconnect
            self._log(f"KAFKA_DELIVERY_ERR event_id={event_id} err={err} "
                      f"(MQTT mid={mqtt_mid} left unacked)")
            return
        self.stats["delivered"] += 1
        self.stats["delivered_payload_bytes"] += payload_bytes
        self._log_delivery({
            "ts": iso_utc_now(),
            "event_id": event_id,
            "kafka_topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "payload_bytes": payload_bytes,
        })
        if self.manual_ack:
            # ack() from this (confluent delivery) thread is safe: paho guards
            # its output packet queue with internal mutexes.
            rc = self.client.ack(mqtt_mid, mqtt_qos)
            if rc == mqtt.MQTT_ERR_SUCCESS:
                self.stats["acked"] += 1
            else:
                self._log(f"MQTT_ACK_ERR mid={mqtt_mid} rc={rc}")

    def _worker(self) -> None:
        while not self.stopping.is_set() or not self.work.empty():
            try:
                message, event_id = self.work.get(timeout=self.poll_interval_s)
            except queue.Empty:
                self.producer.poll(0)
                continue
            payload = message.payload
            sensor_key = None
            try:
                sensor_key = json.loads(payload.decode("utf-8")).get("sensor_id")
            except Exception:
                sensor_key = None
            if sensor_key is None:
                # fall back to the last topic segment so Kafka keys stay stable
                sensor_key = message.topic.rsplit("/", 1)[-1] or "unknown"
            while True:
                try:
                    self.producer.produce(
                        self.kafka_topic,
                        key=sensor_key.encode("utf-8"),
                        value=payload,
                        headers=[("mqtt_topic", message.topic.encode("utf-8"))],
                        on_delivery=lambda err, msg, _eid=event_id, _mid=message.mid,
                        _q=message.qos, _bytes=len(payload): self._delivery_report(
                            err, msg, _eid, _mid, _q, _bytes),
                    )
                    break
                except BufferError:
                    # local producer queue full: serve callbacks and retry
                    self.producer.poll(0.1)
                except ValueError as exc:
                    # e.g. producer closed; drop with log (unacked on MQTT)
                    self._log(f"PRODUCE_ABORT event_id={event_id} err={exc}")
                    break
            self.producer.poll(0)

    # ------------------------------------------------------------------ run --
    def run(self) -> int:
        worker = threading.Thread(target=self._worker, name="kafka-worker", daemon=True)
        worker.start()
        self.client.connect(self.mqtt_broker, self.mqtt_port, keepalive=30)
        self.client.loop_start()
        self._log(f"subscribing {self.topic_filter} qos=1 manual_ack={self.manual_ack} "
                  f"kafka={self.kafka_bootstrap} topic={self.kafka_topic}")

        def _stop(signum, frame):  # noqa: ARG001
            self._log(f"signal {signum}: draining queue and flushing producer")
            self.stopping.set()

        signal.signal(signal.SIGTERM, _stop)
        signal.signal(signal.SIGINT, _stop)

        last_stats = 0.0
        while not self.stopping.is_set():
            time.sleep(0.5)
            self.producer.poll(0)
            now = time.monotonic()
            if now - last_stats >= 30.0:
                last_stats = now
                self._log("stats " + json.dumps(self.stats, separators=(",", ":")))

        # graceful drain: wait for the queue to empty, then flush producer
        deadline = time.monotonic() + 15.0
        while not self.work.empty() and time.monotonic() < deadline:
            self.producer.poll(0.1)
            time.sleep(0.05)
        self.producer.flush(10)
        self.client.loop_stop()
        self.client.disconnect()
        if self._log_fh:
            self._log_fh.close()
        self._log("stopped " + json.dumps(self.stats, separators=(",", ":")))
        return 0


if __name__ == "__main__":
    sys.exit(Bridge().run())
