{% test check__unique(model, column_name, primary_key, severity_level, criticality='NC', custom_where_clause=None) %}

    {% set condition = column_name ~ ' in (select ' ~ column_name ~ ' from ' ~ model ~ ' group by ' ~ column_name ~ ' having count(*) > 1)' %}
    
    {{
        config(
            severity=severity_level
        )
    }}

    {% if execute %}
        {% set test_description = 'This check will fail all records in column `' ~ column_name ~ '` which are not unique' %}
        {{ generate_dq_detail_and_summary(model, column_name, primary_key, severity_level, criticality, 'Unique Check', test_description, condition, custom_where_clause) }}
    {% endif %}

{% endtest %}
