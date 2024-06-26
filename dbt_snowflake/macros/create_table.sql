{% macro create_table(database=target.database, schema=target.schema, table=None, query=None) %}
    {% set create_table_query %}
        create or replace transient table {{ database }}.{{ schema }}.{{ table }} as (
            {{ query }}
        )
    {% endset %}

    {% do run_query(create_table_query) %}
{% endmacro %}
