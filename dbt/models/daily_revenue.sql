-- Purchases and revenue per day and currency (never summed across currencies).
{{ config(materialized='incremental', incremental_strategy='delete+insert',
          unique_key=['event_date', 'currency']) }}

select event_date, currency, count(*) as purchases, sum(amount_cents) as revenue_cents
from {{ ref('fct_events') }}
where event_type = 'purchase'
{% if is_incremental() %}
  and event_date in ({{ touched_dates() }})
{% endif %}
group by 1, 2
