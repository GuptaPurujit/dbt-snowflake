{% test check_not_null(model, column_name, primary_key, severity_level) %}

{{ log (severity_level, true) }}

{% set details_query %}
    select
        '{{ invocation_id }}' as invocation_id,
        TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDDhh24MISS') as timestamp,
        'Null Check' as check_type,
        '{{ severity_level }}' severity,
        '{{ model }}' as table_name,
        '{{ column_name }}' as dq_column_name,
        {{ column_name }} as dq_column_value,
        '{{ primary_key }}' as primary_key_column,
        {{ primary_key }} as primary_key_value
    from
        {{ model }}
    where
        {{ column_name }} is null
{% endset %}

{% set summary_query %}
    select
        '{{ invocation_id }}' as invocation_id,
        TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDDhh24MISS') as timestamp,
        'Null Check' as check_type,
        '{{ severity_level }}' severity,
        '{{ model }}' as table_name,
        '{{ column_name }}' as dq_column_name,
        (select count(*) from {{ model }}) as record_count,
        sum(case when {{ column_name }} is null then 1 else 0 end) failed_record_count
    from
        {{ model }}
    where
        {{ column_name }} is null
    group by
        1, 2, 3, 4
{% endset %}

{{
    config(
      severity = severity_level
    )
}}

{% if execute %}
    {% set detail_results = run_query(details_query) %}

    {% if results | length > 0 %}
        {{ log("No Nulls found for {{ column_name }} in the table {{ model }}", true) }}
        {{ create_table(schema='AUDIT', table='dqm_details', query=details_query ) }}
        {{ create_table(schema='AUDIT', table='dqm_summary', query=summary_query ) }}
        {{ return('') }}
    {% else %}
        {{ create_table(schema='AUDIT', table='dqm_details', query=details_query ) }}
        {{ create_table(schema='AUDIT', table='dqm_summary', query=summary_query ) }}
        {{ summary_query }}

    {% endif %}
{% endif %}

{% endtest %}
