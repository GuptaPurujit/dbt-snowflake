{% macro generate_dq_summary(table, severity_level, criticality, check_type, test_description, column_name, primary_key, failed_record_query) %}
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
            CASE WHEN '{{ severity_level }}' = 'warn' THEN '{{ criticality }}'
                 WHEN '{{ severity_level }}' = 'error' THEN 'C'
                 ELSE NULL
            END as criticality,
            '{{ test_description }}' as test_description,
            '{{ table }}' as table_name,
            '{{ column_name }}' as dq_column_name,
            {{ record_count }} as record_count,
            {{ failed_record_count }} as failed_record_count,
            CASE WHEN {{ failed_record_count }} > 0 THEN 'failed'
                 WHEN {{ failed_record_count }} = 0 THEN 'success'
                 ELSE 'NA'
            END as status
    {% endset %}
    {{ log(dq_summary_insert_query, info=True) }}
    {{ dq_summary_insert_query }}
{% endmacro %}
