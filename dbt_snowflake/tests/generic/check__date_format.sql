{% macro check__date_format(model, column_name, primary_key, severity_level, expected_format, custom_where_clause=None) %}

    {% set condition = "TRY_TO_DATE(" ~ column_name ~ ", '" ~ expected_format ~ "', NULL) IS NULL" %}
    
    {{
        config(
            severity=severity_level
        )
    }}

    {% if execute %}
        {% set where_clause = where_clause(model, column_name, primary_key, condition, custom_where_clause) %}
        
        {{ generate_dq_detail_and_summary(model, column_name, primary_key, severity_level, 'Date Format Check', condition, custom_where_clause) }}
    {% endif %}

{% endmacro %}
