-- Appends the batches this table has not seen yet. "Not seen" is an
-- anti-join on batch_id, not "batch_id > max(batch_id)": several ingest
-- workers commit batches out of id order, and a high-water mark would skip
-- a batch that commits after a higher one was already loaded.
{{ config(materialized='incremental', incremental_strategy='append') }}

select s.*, cast('{{ run_started_at }}' as timestamptz) as loaded_at
from {{ ref('stg_events') }} s
{% if is_incremental() %}
where s.batch_id not in (select distinct batch_id from {{ this }})
{% endif %}
