{% test check__custom_condition(model, column_name, primary_key, severity_level, custom_condition, criticality='NC', custom_where_clause=None) %}

    {% set condition = custom_condition %}
    
    {{
        config(
            severity=severity_level
        )
    }}

    {% if execute %}
        {{ generate_dq_detail_and_summary(model, column_name, primary_key, severity_level, criticality, 'Custom Check', condition, custom_where_clause) }}
    {% endif %}

{% endtest %}
