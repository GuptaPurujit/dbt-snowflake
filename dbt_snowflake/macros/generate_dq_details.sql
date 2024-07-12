{% macro generate_dq_details(table, severity_level, criticality, check_type, column_name, primary_key, where=None) %}
    {% set dq_details_insert_query %}
        select
            '{{ invocation_id }}' as invocation_id,
            TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDDhh24MISS') as timestamp,
            '{{ check_type }}' as check_type,
            '{{ severity_level }}' as severity,
            CASE WHEN '{{ severity_level }}' = 'warn' THEN '{{ criticality }}'
                 WHEN '{{ severity_level }}' = 'error' THEN 'C'
                 ELSE NULL
            END as criticality,
            CASE WHEN '{{ severity_level }}' = 'warn' AND '{{ criticality }}' = 'C' THEN 'Record will be Dropped'
                 ELSE 'Record will be Processed Further'
            END as action,
            '{{ table }}' as table_name,
            '{{ column_name }}' as dq_column_name,
            {{ column_name }} as dq_column_value,
            '{{ primary_key }}' as primary_key_column,
            {{ primary_key }} as primary_key_value
        from
            {{ table }}
        {% if where %} where {{ where }} {% endif %}
    {% endset %}
    {{ dq_details_insert_query }}
{% endmacro %}
