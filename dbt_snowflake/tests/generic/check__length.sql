{% test check__length(model, column_name, primary_key, severity_level, expected_length, operator, criticality='NC', custom_where_clause=None) %}

    {% set condition = 'length(' ~ column_name ~ ') ' ~ operator ~ ' ' ~ expected_length %}

    {{ log(condition, true) }}
    
    {{
        config(
            severity=severity_level
        )
    }}

    {% if execute %}
        {% set test_description = 'This check will fail all records where column `' ~ column_name ~ '` has length ' ~ operator ~ expected_length %}
        {{ generate_dq_detail_and_summary(model, column_name, primary_key, severity_level, criticality, 'Length Check', test_description, condition, custom_where_clause) }}
    {% endif %}

{% endtest %}
