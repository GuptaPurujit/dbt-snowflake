{% test check__date_format(model, column_name, primary_key, severity_level, expected_format, criticality='NC', custom_where_clause=None) %}

    {% set date_format_regex = 
        {
            'YYYY-MM-DD': '^\\\\d{4}-\\\\d{2}-\\\\d{2}$',
            'MM-DD-YYYY': '^\\\\d{2}-\\\\d{2}-\\\\d{4}$',
            'DD-MM-YYYY': '^\\\\d{2}-\\\\d{2}-\\\\d{4}$',
            'DD-MM-YY': '^\\\\d{2}-\\\\d{2}-\\\\d{2}$',
            'YY-MM-DD': '^\\\\d{2}-\\\\d{2}-\\\\d{2}$',
            'MM-DD-YY': '^\\\\d{2}-\\\\d{2}-\\\\d{2}$'
        }
    %}

    {% if expected_format not in date_format_regex %}
        {% do exceptions.raise_compiler_error("Unsupported date format: " ~ expected_format) %}
    {% endif %}

    {% set regex_pattern = date_format_regex[expected_format] %}

    {% set condition = column_name ~ " NOT RLIKE '" ~ regex_pattern ~ "'" %}
    
    {{
        config(
            severity=severity_level
        )
    }}

    {% if execute %}    
        {% set test_description = 'This check will fail all records where the data column `' ~ column_name ~ '` does not match the date format - ' ~ expected_format %} 
        {{ generate_dq_detail_and_summary(model, column_name, primary_key, severity_level, criticality, 'Date Format Check', test_description, condition, custom_where_clause) }}
    {% endif %}

{% endtest %}
