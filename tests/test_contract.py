"""The row contract catches every bad record mixed into the stream, and only those."""

from collections import Counter

import pytest

from lakehouse import codec, contract
from lakehouse.generator import Spec, generate

NOW_MS = 1_800_000_000_000  # 2027-01-15, after the generated window


def _check(raw: bytes) -> str | None:
    try:
        _, e = codec.decode(raw)
    except codec.DecodeError as err:
        return err.reason
    return contract.violation(e, NOW_MS)


def test_injected_bad_records_are_caught_with_the_right_reason():
    caught, missed, false_alarms = Counter(), [], []
    for i, m in enumerate(generate(Spec(n=200_000, bad_rate=0.02))):
        got = _check(m.value)
        if m.kind == "bad":
            if got == m.reason:
                caught[got] += 1
            else:
                missed.append((i, m.reason, got))
        elif got is not None:
            false_alarms.append((i, got))
    assert missed == [] and false_alarms == []
    assert set(caught) == {"undecodable", "unknown_schema", "bad_event_id", "bad_event_type", "bad_user_id",
                           "occurred_at_out_of_range", "bad_amount", "bad_currency"}
    assert min(caught.values()) > 300


GOOD = {"event_id": "6f1c1b54-3a8e-4f7e-9b1e-0a6c2f3d4e5f", "event_type": "purchase", "user_id": 3,
        "occurred_at": 1_790_812_800_000, "amount_cents": 1_999, "currency": "KZT", "session_id": None}


@pytest.mark.parametrize("change, reason", [
    ({}, None),
    ({"event_id": "6F1C1B543A8E4F7E9B1E0A6C2F3D4E5F"}, None),  # other spellings of a UUID are fine
    ({"event_id": "evt-1"}, "bad_event_id"),
    ({"event_type": "Purchase"}, "bad_event_type"),
    ({"user_id": None}, "bad_user_id"),
    ({"user_id": 0}, "bad_user_id"),
    ({"occurred_at": 1_577_836_799_999}, "occurred_at_out_of_range"),  # 1 ms before 2020
    ({"occurred_at": NOW_MS + 5 * 60_000}, None),  # clock skew allowance, inclusive
    ({"occurred_at": NOW_MS + 5 * 60_000 + 1}, "occurred_at_out_of_range"),
    ({"amount_cents": None}, "bad_amount"),
    ({"amount_cents": 0}, "bad_amount"),
    ({"amount_cents": 10_000_000}, None),
    ({"amount_cents": 10_000_001}, "bad_amount"),
    ({"event_type": "page_view"}, "bad_amount"),  # a view with an amount
    ({"event_type": "page_view", "amount_cents": None}, None),
    ({"currency": "kzt"}, "bad_currency"),
    ({"event_type": "clicked", "user_id": None}, "bad_event_type"),  # first broken rule wins
])
def test_rules(change, reason):
    assert contract.violation(GOOD | change, NOW_MS) == reason


def test_event_id_is_normalized():
    e = GOOD | {"event_id": "{6F1C1B54-3A8E-4F7E-9B1E-0A6C2F3D4E5F}"}
    contract.violation(e, NOW_MS)
    assert e["event_id"] == GOOD["event_id"]
