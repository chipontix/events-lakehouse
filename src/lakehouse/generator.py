"""Deterministic synthetic event stream.

Same ``Spec`` -> same bytes, message for message. Besides the Kafka key and
value every message carries the generator's ground truth (``kind``,
``reason``, the canonical event), which ``reference.py`` uses to compute the
expected result without touching Avro, Kafka or the contract code.

Stream time runs from ``start_ms`` over ``days`` days. On top of the clean
events the stream contains:
  * duplicates: an earlier message re-sent byte for byte (producer retry),
    up to ``dup_window`` messages later; same key, so same partition;
  * late events: occurred_at 1 hour to 3 days before current stream time,
    so they land on dates that downstream models have already aggregated;
  * bad records, one broken rule each, labelled with the expected
    quarantine reason;
  * a mix of schema v1 (old tracker) and v2 writers.
"""

import random
import uuid
from collections.abc import Iterator
from dataclasses import dataclass

from . import codec

HOUR_MS = 3_600_000
DAY_MS = 24 * HOUR_MS

BAD_KINDS = (
    "undecodable",
    "unknown_schema",
    "bad_event_id",
    "bad_event_type",
    "bad_user_id",
    "occurred_at_out_of_range",
    "bad_amount",
    "bad_currency",
)


@dataclass(frozen=True)
class Spec:
    n: int = 100_000  # messages, duplicates and bad records included
    seed: int = 42
    dup_rate: float = 0.05
    late_rate: float = 0.03
    bad_rate: float = 0.005
    v1_share: float = 0.3
    users: int = 50_000
    start_ms: int = 1_790_812_800_000  # 2026-10-01T00:00:00Z
    days: int = 7
    dup_window: int = 20_000


@dataclass(frozen=True, slots=True)
class Message:
    key: bytes
    value: bytes
    kind: str  # "event" | "duplicate" | "bad"
    reason: str | None = None  # expected quarantine reason for "bad"
    event: dict | None = None  # canonical (v2-shaped) event for "event"


def _event(rng: random.Random, spec: Spec, stream_ms: int) -> dict:
    r = rng.random()
    etype = "page_view" if r < 0.7 else "add_to_cart" if r < 0.9 else "purchase"
    if rng.random() < spec.late_rate:
        occurred = stream_ms - rng.randint(HOUR_MS, 3 * DAY_MS)
    else:
        occurred = stream_ms + rng.randint(0, 60_000)
    c = rng.random()
    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "event_type": etype,
        "user_id": rng.randint(1, spec.users),
        "occurred_at": occurred,
        "amount_cents": rng.randint(100, 50_000) if etype == "purchase" else None,
        "currency": "USD" if c < 0.8 else "EUR" if c < 0.95 else "KZT",
        "session_id": f"s-{rng.getrandbits(32):08x}" if rng.random() < 0.8 else None,
    }


def _as_v1(e: dict) -> dict:
    return {
        "event_id": e["event_id"],
        "event_type": e["event_type"],
        "user": e["user_id"],
        "ts": e["occurred_at"],
        "amount_cents": e["amount_cents"],
    }


def _encode(e: dict, version: int) -> bytes:
    return codec.encode(_as_v1(e) if version == 1 else e, version)


def _bad(rng: random.Random, e: dict, kind: str) -> bytes:
    version = 2
    if kind == "undecodable":  # a producer that sends JSON instead of Avro
        return b'{"event_id": "%s", "event_type": "page_view"}' % e["event_id"].encode()
    if kind == "unknown_schema":
        return b"\x00\x00\x00\x00\x63" + _encode(e, 2)[5:]
    if kind == "bad_event_id":
        e["event_id"] = "evt-" + e["event_id"][:8]
    elif kind == "bad_event_type":
        e["event_type"] = "clicked"
    elif kind == "bad_user_id":
        e["user_id"] = rng.choice((None, 0, -e["user_id"]))
    elif kind == "occurred_at_out_of_range":
        e["occurred_at"] = rng.choice((1_420_070_400_000, 4_102_444_800_000))  # 2015, 2100
    elif kind == "bad_amount":
        if e["event_type"] == "purchase":
            e["amount_cents"] = rng.choice((None, 0, -e["amount_cents"], 10_000_001))
        else:
            e["amount_cents"] = 500
    elif kind == "bad_currency":
        e["currency"] = "usd"
    return _encode(e, version)


def generate(spec: Spec) -> Iterator[Message]:
    rng = random.Random(spec.seed)
    span = spec.days * DAY_MS
    recent: list[Message] = []  # ring buffer of sent clean events, for duplicates
    for i in range(spec.n):
        stream_ms = spec.start_ms + i * span // spec.n
        r = rng.random()
        if r < spec.dup_rate and recent:
            orig = recent[rng.randrange(len(recent))]
            yield Message(orig.key, orig.value, "duplicate", event=orig.event)
            continue
        e = _event(rng, spec, stream_ms)
        key = str(e["user_id"]).encode()
        if r < spec.dup_rate + spec.bad_rate:
            kind = BAD_KINDS[rng.randrange(len(BAD_KINDS))]
            yield Message(key, _bad(rng, e, kind), "bad", reason=kind)
            continue
        version = 1 if rng.random() < spec.v1_share else 2
        if version == 1:  # v1 cannot carry these; after upcasting they read as the defaults
            e["currency"], e["session_id"] = "USD", None
        m = Message(key, _encode(e, version), "event", event=e)
        if len(recent) < spec.dup_window:
            recent.append(m)
        else:
            recent[i % spec.dup_window] = m
        yield m
