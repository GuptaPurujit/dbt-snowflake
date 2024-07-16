{% macro create_process_logs(step_name) %} 
    {% set query %}
        SELECT
            '{{ invocation_id }}' as invocation_id,
            '{{ step_name }}' as step_name,
            'IN PROGRESS' as status,
            TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDhh24MISS') AS start_time,
            NULL as end_time
    {% endset %}
    
    {{ 
        create_or_update_table(
            schema='AUDIT', 
            table='process_summary',
            query=query,
            insert_flag=True
        ) 
    }}
{% endmacro %}

{% macro update_process_logs(results, step_name) %}
    {% set ns = namespace(overall_status='SUCCESS') %}
    
    {% for result in results %}
        {% if result.status == 'fail' %}
            {% set ns.overall_status = 'FAILED' %}
        {% endif %}
    {% endfor %}

    {% set query %}
        UPDATE {{ database }}.AUDIT.process_summary
        SET
            status = '{{ ns.overall_status }}',
            end_time = TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDhh24MISS')
        WHERE
            invocation_id = '{{ invocation_id }}'
            AND
            step_name = '{{ step_name }}'
    {% endset %}
    
    {{ 
        create_or_update_table(
            schema='AUDIT', 
            table='process_summary',
            query=query,
            insert_flag=False
        ) 
    }}
{% endmacro %}
