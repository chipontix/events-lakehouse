"""Sends a slice of the generated stream to Kafka."""

import itertools
import time

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .generator import Spec, generate


def ensure_topic(bootstrap: str, topic: str, partitions: int) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=10).topics:
        return
    fut = admin.create_topics([NewTopic(topic, partitions, 1)])[topic]
    try:
        fut.result()
    except KafkaException as e:
        if "TOPIC_ALREADY_EXISTS" not in str(e):
            raise


def run(bootstrap: str, topic: str, spec: Spec, start: int, stop: int | None) -> dict:
    p = Producer({
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,  # acks=all, no reordering or duplicates from retries
        "linger.ms": 20,
        "batch.size": 1_000_000,
        "compression.type": "lz4",
    })
    errors = []

    def on_delivery(err, _msg):
        if err:
            errors.append(err)

    t0 = time.monotonic()
    sent = 0
    for m in itertools.islice(generate(spec), start, stop):
        while True:
            try:
                p.produce(topic, m.value, m.key, on_delivery=on_delivery)
                break
            except BufferError:
                p.poll(0.05)
        p.poll(0)
        sent += 1
    p.flush()
    secs = time.monotonic() - t0
    if errors:
        raise RuntimeError(f"{len(errors)} delivery errors, first: {errors[0]}")
    return {"messages": sent, "seconds": round(secs, 3), "messages_per_s": round(sent / secs)}
