{% test check__regex_match(model, column_name, primary_key, severity_level, regex_pattern, criticality='NC', custom_where_clause=None) %}

    {% set condition = column_name ~ ' NOT RLIKE ' ~ regex_pattern %}
    
    {{
        config(
            severity=severity_level
        )
    }}

    {% if execute %}
        {{ generate_dq_detail_and_summary(model, column_name, primary_key, severity_level, criticality, 'Regex Match Check', condition, custom_where_clause) }}
    {% endif %}

{% endtest %}
