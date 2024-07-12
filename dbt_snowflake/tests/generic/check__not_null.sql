{% test check__not_null(model, column_name, primary_key, severity_level, criticality='NC', custom_where_clause=None) %}

    {% set condition = column_name ~ ' is null' %}
    
    {{
        config(
            severity=severity_level
        )
    }}

    {% if execute %}
        {% set test_description = 'This check will fail all records where column `' ~ column_name ~ '` is NULL' %} 
        {{ generate_dq_detail_and_summary(model, column_name, primary_key, severity_level, criticality, 'Null Check', test_description, condition, custom_where_clause) }}
    {% endif %}

{% endtest %}
