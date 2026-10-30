{#- Generic test: fails on rows where the column is outside [min, max].
    NULLs pass (not_null covers them); `where` narrows the rows checked. -#}
{% test in_range(model, column_name, min, max) %}
select * from {{ model }}
where {{ column_name }} < {{ min }} or {{ column_name }} > {{ max }}
{% endtest %}
