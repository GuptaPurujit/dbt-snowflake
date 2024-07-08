{% macro generate_dq_details(table, severity_level, check_type, column_name, primary_key, where=None) %}
    {% set dq_details_insert_query %}
        select
            '{{ invocation_id }}' as invocation_id,
            TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDDhh24MISS') as timestamp,
            '{{ check_type }}' as check_type,
            '{{ severity_level }}' as severity,
            '{{ table }}' as table_name,
            '{{ column_name }}' as dq_column_name,
            {{ column_name }} as dq_column_value,
            '{{ primary_key }}' as primary_key_column,
            {{ primary_key }} as primary_key_value
        from
            {{ table }}
        {% if where %} where {{ where }} {% endif %}
    {% endset %}
    {{ log(dq_details_insert_query, true) }}
    {{ dq_details_insert_query }}
{% endmacro %}
