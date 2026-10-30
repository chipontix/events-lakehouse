{#- Event dates that received rows in this dbt run. Late events make this
    a scattered set of old dates, not "today". -#}
{% macro touched_dates() %}
    select distinct event_date from {{ ref('fct_events') }}
    where loaded_at = cast('{{ run_started_at }}' as timestamptz)
{% endmacro %}
