import io
import json

import fastavro
import pytest

from lakehouse import codec


def test_registry_has_two_versions_and_v2_is_latest():
    assert sorted(codec.SCHEMAS) == [1, 2] and codec.LATEST == 2


def test_v1_message_reads_in_v2_shape_with_defaults():
    raw = codec.encode({"event_id": "e", "event_type": "purchase", "user": 7, "ts": 1_000, "amount_cents": 250}, 1)
    assert codec.decode(raw) == (1, {
        "event_id": "e", "event_type": "purchase", "user_id": 7, "occurred_at": 1_000,
        "amount_cents": 250, "currency": "USD", "session_id": None,
    })


def test_v2_roundtrip():
    e = {"event_id": "e", "event_type": "page_view", "user_id": 1, "occurred_at": 5,
         "amount_cents": None, "currency": "EUR", "session_id": "s-1"}
    assert codec.decode(codec.encode(e, 2)) == (2, e)


@pytest.mark.parametrize("raw, reason", [
    (b'{"event_id": "x"}', "undecodable"),
    (b"\x00\x00\x00\x00\x09" + b"\x02x", "unknown_schema"),
    (b"\x00\x00\x00\x00\x02" + b"\x7f", "undecodable"),  # truncated body
    (b"", "undecodable"),
])
def test_decode_failures_carry_a_reason(raw, reason):
    with pytest.raises(codec.DecodeError) as e:
        codec.decode(raw)
    assert e.value.reason == reason


def test_every_registered_version_resolves_to_latest():
    """The compatibility gate: data written with any old schema stays readable."""
    for version, writer in codec.SCHEMAS.items():
        sample = {f["name"]: _sample(f["type"]) for f in writer["fields"]}
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, writer, sample)
        buf.seek(0)
        fastavro.schemaless_reader(buf, writer, codec.SCHEMAS[codec.LATEST])


def test_a_breaking_change_is_caught():
    """A v3 that adds a required field without a default cannot read v2 data."""
    v3 = json.loads(json.dumps(codec.SCHEMAS[2]))
    v3["fields"].append({"name": "country", "type": "string"})
    e = {"event_id": "e", "event_type": "page_view", "user_id": 1, "occurred_at": 5,
         "amount_cents": None, "currency": "EUR", "session_id": None}
    raw = codec.encode(e, 2)[5:]
    with pytest.raises(fastavro.read.SchemaResolutionError):
        fastavro.schemaless_reader(io.BytesIO(raw), codec.SCHEMAS[2], fastavro.parse_schema(v3))


def _sample(t):
    t = t[-1] if isinstance(t, list) else t
    return {"string": "x", "long": 1, "int": 1}[t]
