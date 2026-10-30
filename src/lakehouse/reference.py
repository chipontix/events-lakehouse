"""Expected pipeline output, computed from the generator's ground truth.

Independent of the system under test: no Avro decoding, no Kafka, no
contract code, no SQL. It only trusts the labels the generator attached when
it built each message.
"""

import datetime as dt
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from hashlib import blake2b

from .generator import Spec, generate

MASK = (1 << 64) - 1


def row_hash(event_id, event_type, user_id, occurred_at_ms, amount_cents, currency, session_id) -> int:
    s = f"{event_id}|{event_type}|{user_id}|{occurred_at_ms}|{amount_cents}|{currency}|{session_id}"
    return int.from_bytes(blake2b(s.encode(), digest_size=8).digest())


def event_date(occurred_at_ms: int) -> dt.date:
    return dt.datetime.fromtimestamp(occurred_at_ms / 1000, dt.UTC).date()


@dataclass
class Result:
    """Comparable summary of a pipeline output (or of the expectation)."""

    events: int = 0
    digest: int = 0  # order-independent: sum of row hashes mod 2^64
    daily: dict = field(default_factory=dict)  # (date, event_type) -> (events, distinct users)
    revenue: dict = field(default_factory=dict)  # (date, currency) -> (purchases, revenue_cents)
    quarantine: dict = field(default_factory=dict)  # reason -> messages
    duplicates: int = 0

    def add_row(self, *row) -> None:
        self.events += 1
        self.digest = (self.digest + row_hash(*row)) & MASK


def expected(spec: Spec) -> Result:
    res = Result()
    quarantine: Counter = Counter()
    counts: Counter = Counter()
    users = defaultdict(set)
    revenue = defaultdict(lambda: [0, 0])
    for m in generate(spec):
        if m.kind == "duplicate":
            res.duplicates += 1
            continue
        if m.kind == "bad":
            quarantine[m.reason] += 1
            continue
        e = m.event
        res.add_row(e["event_id"], e["event_type"], e["user_id"], e["occurred_at"],
                    e["amount_cents"], e["currency"], e["session_id"])
        d = event_date(e["occurred_at"])
        counts[d, e["event_type"]] += 1
        users[d, e["event_type"]].add(e["user_id"])
        if e["event_type"] == "purchase":
            r = revenue[d, e["currency"]]
            r[0] += 1
            r[1] += e["amount_cents"]
    res.daily = {k: (counts[k], len(users[k])) for k in counts}
    res.revenue = {k: tuple(v) for k, v in revenue.items()}
    res.quarantine = dict(quarantine)
    return res
