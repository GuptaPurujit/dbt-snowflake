{% test check__lov(model, column_name, primary_key, severity_level, allowed_values, criticality='NC', custom_where_clause=None) %}

    {% set condition = column_name ~ " NOT IN " ~ allowed_values %}
    
    {{
        config(
            severity=severity_level
        )
    }}

    {% if execute %}
        {% set test_description = 'This check will fail all records where column `' ~ column_name ~ '` does not have values in ' ~ allowed_values.replace("'", "''") %} 
        {{ generate_dq_detail_and_summary(model, column_name, primary_key, severity_level, criticality, 'List of Values Check', test_description, condition, custom_where_clause) }}
    {% endif %}

{% endtest %}