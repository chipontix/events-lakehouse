"""Row-level data contract, applied in the stream before anything is written.

A record that breaks it goes to ``events_quarantine`` with the reason; the
first broken rule wins. ``violation`` also normalizes ``event_id`` to the
canonical lowercase UUID text, so that the dedup key has one spelling. Table-level checks (uniqueness across batches,
freshness) live in dbt, see ``dbt/models/schema.yml``.
"""

import re
import uuid

EVENT_TYPES = frozenset({"page_view", "add_to_cart", "purchase"})
MIN_OCCURRED_AT_MS = 1_577_836_800_000  # 2020-01-01T00:00:00Z
MAX_CLOCK_SKEW_MS = 5 * 60 * 1000
MAX_AMOUNT_CENTS = 10_000_000
_CURRENCY = re.compile(r"[A-Z]{3}")


def violation(e: dict, now_ms: int) -> str | None:
    try:
        e["event_id"] = str(uuid.UUID(e["event_id"]))
    except ValueError:
        return "bad_event_id"
    if e["event_type"] not in EVENT_TYPES:
        return "bad_event_type"
    if e["user_id"] is None or e["user_id"] <= 0:
        return "bad_user_id"
    if not MIN_OCCURRED_AT_MS <= e["occurred_at"] <= now_ms + MAX_CLOCK_SKEW_MS:
        return "occurred_at_out_of_range"
    amount = e["amount_cents"]
    if e["event_type"] == "purchase":
        if amount is None or not 1 <= amount <= MAX_AMOUNT_CENTS:
            return "bad_amount"
    elif amount is not None:
        return "bad_amount"
    if not _CURRENCY.fullmatch(e["currency"]):
        return "bad_currency"
    return None
