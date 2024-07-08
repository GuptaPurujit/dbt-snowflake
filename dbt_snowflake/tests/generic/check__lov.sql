{% test check__lov(model, column_name, primary_key, severity_level, allowed_values, custom_where_clause=None) %}

    {% set condition = column_name ~ " NOT IN " ~ allowed_values %}
    
    {{
        config(
            severity=severity_level
        )
    }}

    {% if execute %}     
        {{ generate_dq_detail_and_summary(model, column_name, primary_key, severity_level, 'List of Values Check', condition, custom_where_clause) }}
    {% endif %}

{% endtest %}