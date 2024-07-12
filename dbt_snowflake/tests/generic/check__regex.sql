{% test check__regex_match(model, column_name, primary_key, severity_level, regex_pattern, criticality='NC', custom_where_clause=None) %}

    {% set condition = column_name ~ ' NOT RLIKE ' ~ regex_pattern %}
    
    {{
        config(
            severity=severity_level
        )
    }}

    {% if execute %}
        {% set test_description = 'This check will fail all records where column `' ~ column_name ~ '` does not match the regex expression - ' ~ regex_pattern.replace("'", "''") %}
        {{ generate_dq_detail_and_summary(model, column_name, primary_key, severity_level, criticality, 'Regex Match Check', test_description, condition, custom_where_clause) }}
    {% endif %}

{% endtest %}
