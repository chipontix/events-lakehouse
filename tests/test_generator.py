import hashlib
from collections import Counter

from lakehouse.generator import BAD_KINDS, DAY_MS, Spec, generate
from lakehouse.reference import event_date


def _digest(spec):
    h = hashlib.sha256()
    for m in generate(spec):
        h.update(m.key + b"\0" + m.value)
    return h.hexdigest()


def test_same_seed_same_bytes():
    assert _digest(Spec(n=5_000, seed=7)) == _digest(Spec(n=5_000, seed=7))
    assert _digest(Spec(n=5_000, seed=7)) != _digest(Spec(n=5_000, seed=8))


def test_stream_has_the_promised_mix():
    spec = Spec(n=100_000)
    msgs = list(generate(spec))
    kinds = Counter(m.kind for m in msgs)
    assert 0.045 < kinds["duplicate"] / spec.n < 0.055
    assert 0.004 < kinds["bad"] / spec.n < 0.006
    assert {m.reason for m in msgs if m.kind == "bad"} == set(BAD_KINDS)
    assert Counter(m.value[4] for m in msgs if m.kind == "event").keys() == {1, 2}  # both writer schemas

    # late: occurred more than an hour before the stream position it was sent at
    span = spec.days * DAY_MS
    late = [m for i, m in enumerate(msgs) if m.kind == "event"
            and m.event["occurred_at"] < spec.start_ms + i * span // spec.n - 3_600_000]
    assert 0.025 < len(late) / kinds["event"] < 0.035


def test_duplicates_repeat_an_earlier_message_on_the_same_key():
    seen = {}
    for m in generate(Spec(n=20_000)):
        if m.kind == "event":
            seen[m.value] = m.key
        elif m.kind == "duplicate":
            assert seen[m.value] == m.key


def test_late_events_reach_dates_the_first_half_already_covered():
    """What the two-phase pipeline test relies on: phase 2 brings rows for
    dates phase 1 already aggregated."""
    spec = Spec(n=50_000)
    msgs = list(generate(spec))
    half = spec.n // 2
    first = {event_date(m.event["occurred_at"]) for m in msgs[:half] if m.kind == "event"}
    second = {event_date(m.event["occurred_at"]) for m in msgs[half:] if m.kind == "event"}
    assert first & second
