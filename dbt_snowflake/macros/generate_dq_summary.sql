{% macro generate_dq_summary(table, severity_level, check_type, column_name, primary_key, failed_record_query) %}
    {% set row_count_query %}
        select
            count(*) as row_count 
        from
            {{ table }}
    {% endset %}

    {% set result = run_query(row_count_query) %}
    {% set failed_record_count_res = run_query(failed_record_query) %}
    
    {% if execute %}
        {% set record_count = result.rows[0].values()[0] %}
        {{ log(msg="Total Record Count:  " ~ record_count, info=true) }}
        {% set failed_record_count = failed_record_count_res.rows[0].values()[0] %}
        {{ log(msg="Failed Record Count:  " ~ failed_record_count, info=true) }}
    {% endif %}
    {% set dq_summary_insert_query %}
        select
            '{{ invocation_id }}' as invocation_id,
            TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDDhh24MISS') as timestamp,
            '{{ check_type }}' as check_type,
            '{{ severity_level }}' as severity,
            '{{ table }}' as table_name,
            '{{ column_name }}' as dq_column_name,
            {{ record_count }} as record_count,
            {{ failed_record_count }} as failed_record_count
    {% endset %}
    {{ dq_summary_insert_query }}
{% endmacro %}
