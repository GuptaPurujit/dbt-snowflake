{% test check__unique(model, column_name, primary_key, severity_level) %}

    {% set condition = column_name ~ ' in (select ' ~ column_name ~ ' from ' ~ model ~ ' group by ' ~ column_name ~ ' having count(*) > 1)' %}
    
    {{
        config(
            severity=severity_level
        )
    }}

    {% if execute %}
        {{ generate_dq_detail_and_summary(model, column_name, primary_key, severity_level, 'Unique Check', condition) }}
    {% endif %}

{% endtest %}
