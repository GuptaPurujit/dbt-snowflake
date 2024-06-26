{% macro create_or_update_table(database=target.database, schema=target.schema, table=None, query=None) %}
    {% set check_table_query %}
        SELECT 1
        FROM SNOWFLAKE.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_CATALOG = '{{ database }}'
            AND TABLE_SCHEMA = '{{ schema }}'
            AND TABLE_NAME = '{{ table }}'
        LIMIT 1
    {% endset %}

    {% set result = run_query(check_table_query) %}
    {% if execute %}
        {% if result.rows | length > 0 %}
            {% set table_exists = true %}
        {% else %}
            {% set table_exists = false %}
        {% endif %}
    {% endif %}

    {% if table_exists %}
        {% set insert_query %}
            INSERT INTO {{ database }}.{{ schema }}.{{ table }}
            ({{ query }})
        {% endset %}
        {{ log("Table Already Exists, hence inserting new records", true) }}
        {% do run_query(insert_query) %}
    {% else %}
        {% set create_table_query %}
            CREATE OR REPLACE TRANSIENT TABLE {{ database }}.{{ schema }}.{{ table }} AS (
                {{ query }}
            )
        {% endset %}
        {{ log("Table Does not Exists, hence creating table", true) }}
        {% do run_query(create_table_query) %}
    {% endif %}
{% endmacro %}
