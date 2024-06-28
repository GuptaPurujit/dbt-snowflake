{% test check__not_null(model, column_name, primary_key, severity_level, custom_where_clause=None) %}

    {% set condition = column_name ~ ' is null' %}
    
    {{
        config(
            severity=severity_level
        )
    }}

    {% if execute %}
        {{ generate_dq_detail_and_summary(model, column_name, primary_key, severity_level, 'Null Check', condition, custom_where_clause) }}
    {% endif %}

{% endtest %}
