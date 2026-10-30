"""Wire format and schema registry.

A message value is ``0x00 | schema id (uint32, big endian) | Avro body``, the
same framing as the Confluent serializers. The registry is the ``schemas/``
directory: ``event.v<id>.avsc``. Every message is decoded with its writer
schema and resolved to the latest schema, so a v1 record comes out with v2
field names and defaults.
"""

import io
import json
import struct
from importlib.resources import files

import fastavro

_HEADER = struct.Struct(">bI")


def _load() -> dict[int, dict]:
    out = {}
    for f in files("lakehouse").joinpath("schemas").iterdir():
        if f.name.startswith("event.v") and f.name.endswith(".avsc"):
            out[int(f.name[7:-5])] = fastavro.parse_schema(json.loads(f.read_text()))
    return out


SCHEMAS = _load()
LATEST = max(SCHEMAS)


class DecodeError(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def encode(record: dict, version: int) -> bytes:
    buf = io.BytesIO()
    buf.write(_HEADER.pack(0, version))
    fastavro.schemaless_writer(buf, SCHEMAS[version], record)
    return buf.getvalue()


def decode(raw: bytes) -> tuple[int, dict]:
    """Returns (writer schema id, record in the latest schema's shape)."""
    if len(raw) < _HEADER.size or raw[0] != 0:
        raise DecodeError("undecodable")
    version = int.from_bytes(raw[1:5])
    writer = SCHEMAS.get(version)
    if writer is None:
        raise DecodeError("unknown_schema")
    reader = None if version == LATEST else SCHEMAS[LATEST]
    try:
        return version, fastavro.schemaless_reader(io.BytesIO(raw[5:]), writer, reader)
    except Exception as e:  # fastavro raises a zoo of types on bad bytes
        raise DecodeError("undecodable") from e
