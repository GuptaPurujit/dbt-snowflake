{% test check__length(model, column_name, primary_key, severity_level, expected_length, operator, custom_where_clause=None) %}

    {% set condition = 'length(' ~ column_name ~ ') ' ~ operator ~ ' ' ~ expected_length %}

    {{ log(condition, true) }}
    
    {{
        config(
            severity=severity_level
        )
    }}

    {% if execute %}
        {{ generate_dq_detail_and_summary(model, column_name, primary_key, severity_level, 'Length Check', condition, custom_where_clause) }}
    {% endif %}

{% endtest %}
