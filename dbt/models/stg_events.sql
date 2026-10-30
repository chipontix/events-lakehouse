-- One row per event as landed. A view: the files are the storage.
select
    event_id,
    event_type,
    user_id,
    occurred_at,
    cast(occurred_at as date) as event_date,  -- occurred_at is UTC, the session time zone is UTC (see profiles.yml)
    amount_cents,
    currency,
    session_id,
    schema_version,
    batch_id,
    ingested_at
from {{ source('lake', 'events') }}
