-- Events and distinct users per day and type. Distinct counts cannot be
-- merged from partial results, so every day that got new rows (late events
-- included) is recomputed from fct_events and replaced.
{{ config(materialized='incremental', incremental_strategy='delete+insert',
          unique_key=['event_date', 'event_type']) }}

select event_date, event_type, count(*) as events, count(distinct user_id) as users
from {{ ref('fct_events') }}
{% if is_incremental() %}
where event_date in ({{ touched_dates() }})
{% endif %}
group by 1, 2
