"""Exercise the real bridge worker with callback-gated MQTT inflight windows.

Kafka callbacks become ready after linger.ms; only poll() serves them. A
virtual queue advances time when empty, so missed MQTT replenishment is
measured deterministically without Docker, network clients, or sleeps.
"""

import importlib.util
import io
import json
import os
import queue
import sys
import types
import unittest
from fractions import Fraction
from pathlib import Path
from unittest.mock import patch


class FakeMQTTClient:
    def __init__(self, **options):
        self.options = options
        self.acks = []
        self.trace = []
        self.broker = None

    def ack(self, mid, qos):
        self.trace.append(("ack", mid, qos))
        self.acks.append((mid, qos))
        self.broker.ack(mid)
        return 0


class FakeProducer:
    def __init__(self, config):
        self.config = config
        self.produced = []
        self.pending = []
        self.poll_calls = []
        self.trace = []
        self.errors = {}
        self.clock = lambda: Fraction(0)

    def produce(self, topic, **options):
        offset = len(self.produced)
        self.produced.append((topic, options))
        message = types.SimpleNamespace(
            topic=lambda: topic, partition=lambda: 0, offset=lambda: offset,
        )
        ready_at = self.clock() + Fraction(self.config["linger.ms"], 1000)
        self.pending.append((ready_at, offset, options["on_delivery"], message))

    def poll(self, timeout):
        self.poll_calls.append(timeout)
        ready = [item for item in self.pending if item[0] <= self.clock()]
        self.pending = [item for item in self.pending if item[0] > self.clock()]
        for _, offset, callback, message in ready:
            error = self.errors.get(offset)
            self.trace.append(("callback", offset + 1, error))
            callback(error, message)
        return len(ready)


def load_bridge():
    """Keep optional client stubs local to this import, not other test modules."""
    modules = {
        name: types.ModuleType(name)
        for name in ("paho", "paho.mqtt", "paho.mqtt.client", "confluent_kafka")
    }
    modules["paho"].__path__ = []
    modules["paho.mqtt"].__path__ = []
    modules["paho"].mqtt = modules["paho.mqtt"]
    modules["paho.mqtt"].client = modules["paho.mqtt.client"]
    mqtt = modules["paho.mqtt.client"]
    mqtt.Client = FakeMQTTClient
    mqtt.CallbackAPIVersion = types.SimpleNamespace(VERSION2=2)
    mqtt.MQTTv311 = 4
    mqtt.MQTT_ERR_SUCCESS = 0
    modules["confluent_kafka"].Producer = FakeProducer
    path = Path(__file__).resolve().parents[1] / "bridge" / "bridge.py"
    spec = importlib.util.spec_from_file_location("bridge_polling_target", path)
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, modules):
        spec.loader.exec_module(module)
    return module


bridge_module = load_bridge()


class VirtualQueue(queue.Queue):
    def __init__(self, bridge, deadline):
        super().__init__(maxsize=bridge.queue_size)
        self.bridge = bridge
        self.now = Fraction(0)
        self.deadline = Fraction(deadline)
        self.waits = []

    def get(self, block=True, timeout=None):
        try:
            return super().get(block=False)
        except queue.Empty:
            if not block:
                raise
            if timeout is None or timeout <= 0:
                raise AssertionError("Idle worker must use a positive bounded wait")
            self.waits.append(timeout)
            self.now += Fraction(str(timeout))
            if self.now >= self.deadline:
                self.bridge.stopping.set()
            raise


class FakeMQTTBroker:
    """Already published messages wait for subscriber PUBACK to free slots."""
    def __init__(self, bridge, total, window):
        self.bridge = bridge
        self.total = total
        self.window = window
        self.next_mid = 1
        self.inflight = set()
        self.peak_inflight = 0
        self.messages = []
        bridge.client.broker = self
        self.replenish()

    def replenish(self):
        while (self.next_mid <= self.total and len(self.inflight) < self.window
               and not self.bridge.stopping.is_set()):
            mid = self.next_mid
            self.next_mid += 1
            self.inflight.add(mid)
            self.peak_inflight = max(self.peak_inflight, len(self.inflight))
            message = types.SimpleNamespace(
                mid=mid, qos=1, topic="sensors/sensor-1/telemetry",
                payload=json.dumps({
                    "event_id": f"event-{mid}", "sensor_id": "sensor-1",
                }).encode("utf-8"),
            )
            self.messages.append(message)
            self.bridge._on_message(self.bridge.client, None, message)

    def ack(self, mid):
        self.inflight.remove(mid)
        if self.next_mid > self.total and not self.inflight:
            self.bridge.stopping.set()
        else:
            self.replenish()


class BridgePollingTests(unittest.TestCase):
    def setUp(self):
        env = patch.dict(os.environ, {}, clear=True)
        env.start()
        self.addCleanup(env.stop)

    def make_bridge(self, total=0, window=2, deadline="0.08"):
        bridge = bridge_module.Bridge()
        bridge.work = VirtualQueue(bridge, deadline)
        bridge._log_fh = io.StringIO()
        bridge.producer.clock = lambda: bridge.work.now
        bridge.producer.trace = bridge.client.trace
        broker = FakeMQTTBroker(bridge, total, window)
        return bridge, broker

    def records(self, bridge):
        return [json.loads(line) for line in bridge._log_fh.getvalue().splitlines()]

    def test_idle_callbacks_replenish_finite_inflight_window(self):
        bridge, broker = self.make_bridge(total=6)
        bridge._worker()

        self.assertEqual(
            bridge.stats["delivered"], 6,
            "Idle callback polling must release all three MQTT inflight windows",
        )
        self.assertEqual(bridge.client.acks, [(mid, 1) for mid in range(1, 7)])
        self.assertEqual(broker.peak_inflight, 2)
        self.assertFalse(broker.inflight)
        self.assertFalse(bridge.producer.pending)
        self.assertTrue(bridge.stopping.is_set())
        self.assertEqual(bridge.client.trace, [
            entry for mid in range(1, 7)
            for entry in (("callback", mid, None), ("ack", mid, 1))
        ])
        records = self.records(bridge)
        self.assertEqual([row["event_id"] for row in records],
                         [f"event-{mid}" for mid in range(1, 7)])
        self.assertEqual([row["offset"] for row in records], list(range(6)))
        for row, message in zip(records, broker.messages):
            self.assertEqual(set(row), {
                "ts", "event_id", "kafka_topic", "partition", "offset", "payload_bytes",
            })
            self.assertEqual(row["kafka_topic"], "sensor_raw")
            self.assertEqual(row["partition"], 0)
            self.assertEqual(row["payload_bytes"], len(message.payload))
        self.assertEqual(bridge.stats["delivered_payload_bytes"],
                         sum(len(message.payload) for message in broker.messages))
        self.assertEqual(bridge.stats["acked"], 6)
        self.assertEqual(bridge.stats["dropped_queue_full"], 0)
        self.assertTrue(bridge.client.options["manual_ack"])
        self.assertFalse(bridge.client.options["clean_session"])
        self.assertEqual(bridge.producer.config["acks"], "all")
        self.assertTrue(bridge.producer.config["enable.idempotence"])
        self.assertEqual(bridge.producer.config["linger.ms"], 20)

    def test_custom_interval_services_callbacks_while_idle(self):
        os.environ["BRIDGE_POLL_INTERVAL_S"] = "0.02"
        bridge, _ = self.make_bridge(total=6)
        bridge._worker()
        self.assertEqual(bridge.stats["delivered"], 6)
        self.assertEqual(bridge.work.waits, [0.02, 0.02, 0.02])

    def test_idle_worker_waits_and_stops_without_spinning(self):
        bridge, _ = self.make_bridge(deadline="0.03")
        bridge._worker()
        self.assertEqual(bridge.work.waits, [0.01, 0.01, 0.01])
        self.assertEqual(bridge.producer.poll_calls, [0, 0, 0])
        self.assertTrue(bridge.stopping.is_set())
        self.assertEqual(bridge.stats["received"], 0)

    def test_stopping_worker_drains_already_queued_messages(self):
        bridge, _ = self.make_bridge(total=2)
        bridge.stopping.set()
        # Kafka is already ready, so post-produce polling can deliver immediately.
        bridge.producer.clock = lambda: Fraction(1)
        bridge.producer.config["linger.ms"] = 0
        bridge._worker()
        self.assertTrue(bridge.work.empty())
        self.assertEqual(bridge.work.waits, [])
        self.assertEqual(bridge.stats["delivered"], 2)
        self.assertEqual(bridge.client.acks, [(1, 1), (2, 1)])

    def test_failed_kafka_callback_keeps_mqtt_message_unacked(self):
        bridge, broker = self.make_bridge(total=2)
        bridge.producer.errors[1] = "delivery failed"
        with patch.object(bridge, "_log") as log:
            bridge._worker()
        self.assertEqual(bridge.client.acks, [(1, 1)])
        self.assertEqual(broker.inflight, {2})
        self.assertEqual(bridge.stats["delivered"], 1)
        self.assertEqual(bridge.stats["delivery_errors"], 1)
        self.assertEqual([row["event_id"] for row in self.records(bridge)], ["event-1"])
        log.assert_called_once_with(
            "KAFKA_DELIVERY_ERR event_id=event-2 err=delivery failed "
            "(MQTT mid=2 left unacked)"
        )

    def test_default_interval(self):
        self.assertEqual(bridge_module.Bridge().poll_interval_s, 0.01)

    def test_valid_interval_bounds(self):
        for value in ("0.001", "0.02", "0.5"):
            with self.subTest(value=value):
                os.environ["BRIDGE_POLL_INTERVAL_S"] = value
                self.assertEqual(bridge_module.Bridge().poll_interval_s, float(value))

    def test_invalid_intervals_fail_before_clients_are_created(self):
        for value in ("", "bad", "0", "-0.01", "nan", "inf", "-inf", "0.500001", "1e309"):
            with self.subTest(value=value):
                os.environ["BRIDGE_POLL_INTERVAL_S"] = value
                with patch.object(bridge_module.mqtt, "Client") as client:
                    with patch.object(bridge_module, "Producer") as producer:
                        with self.assertRaisesRegex(ValueError, "BRIDGE_POLL_INTERVAL_S"):
                            bridge_module.Bridge()
                        client.assert_not_called()
                        producer.assert_not_called()


if __name__ == "__main__":
    unittest.main()
