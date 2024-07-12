{% macro get_error_record_count_query(model, column_name, condition, custom_where_clause) %}
    select
        count(*) as failed_record_count
    from
        {{ model }}
    where
        {{ condition }}
        {% if custom_where_clause is not none %}
        and {{ custom_where_clause }}
        {% endif %}
{% endmacro %}

{% macro where_clause(model, column_name, primary_key, condition, custom_where_clause) %}
    {{ primary_key }} in (
        select
            {{ primary_key }}
        from
            {{ model }}
        where
            {{ condition }}
            {% if custom_where_clause is not none %}
            and {{ custom_where_clause }}
            {% endif %}
    )
{% endmacro %}

{% macro generate_dq_detail_and_summary(model, column_name, primary_key, severity_level, criticality, check_type, condition, custom_where_clause) %}
    {% set where_clause = where_clause(model, column_name, primary_key, condition, custom_where_clause) %}
    {% set detail_results = run_query(generate_dq_details(model, severity_level, criticality, check_type, column_name, primary_key, where_clause) ) %}

    {% if detail_results | length == 0 %}
        {{ log(msg="No issues found for " ~ column_name ~ " in the table " ~ model, info=true) }}
        {{ 
            create_or_update_table(
                schema='AUDIT', 
                table='dqm_summary', 
                query=generate_dq_summary(
                        table=model, 
                        severity_level=severity_level,
                        criticality=criticality, 
                        check_type=check_type, 
                        column_name=column_name, 
                        primary_key=primary_key, 
                        failed_record_query='select 0'
                )
            ) 
        }}
        select * from (select 1 as id where 1 = 0)
    {% else %}
        {{ 
            create_or_update_table(
                schema='AUDIT', 
                table='dqm_details', 
                query=generate_dq_details(
                        table=model, 
                        severity_level=severity_level,
                        criticality=criticality, 
                        check_type=check_type, 
                        column_name=column_name, 
                        primary_key=primary_key, 
                        where=where_clause
                )
            ) 
        }}
        {{ 
            create_or_update_table(
                schema='AUDIT', 
                table='dqm_summary', 
                query=generate_dq_summary(
                        table=model, 
                        severity_level=severity_level,
                        criticality=criticality,
                        check_type=check_type, 
                        column_name=column_name, 
                        primary_key=primary_key, 
                        failed_record_query=get_error_record_count_query(model, column_name, condition, custom_where_clause)
                )
            ) 
        }}
        select 1
    {% endif %}
{% endmacro %}
