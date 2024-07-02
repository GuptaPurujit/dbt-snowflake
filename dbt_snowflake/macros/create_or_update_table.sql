{% macro create_or_update_table(database=target.database, schema=target.schema, table=None, query=None) %}
    {% set table_exists = adapter.get_relation(database=database, schema=schema, identifier=table) is not none %}
    {% set insert_query %}
            INSERT INTO {{ database }}.{{ schema }}.{{ table }}
            ({{ query }})
        {% endset %}
    {% if not table_exists %}
        {% set create_table_query %}
            {% if table == 'dqm_summary' %}
                create TRANSIENT TABLE {{ database }}.{{ schema }}.{{ table }} (
                    INVOCATION_ID VARCHAR(36),
                    TIMESTAMP TIMESTAMP_NTZ(9),
                    CHECK_TYPE VARCHAR(100),
                    SEVERITY VARCHAR(5),
                    TABLE_NAME VARCHAR(100),
                    DQ_COLUMN_NAME VARCHAR(100),
                    RECORD_COUNT NUMBER(38,0),
                    FAILED_RECORD_COUNT NUMBER(38,0),
                    STATUS VARCHAR(10)
                );
            {% elif table == 'dqm_details' %}
                create TRANSIENT TABLE {{ database }}.{{ schema }}.{{ table }} (
                    INVOCATION_ID VARCHAR(36),
                    TIMESTAMP TIMESTAMP_NTZ(9),
                    CHECK_TYPE VARCHAR(100),
                    SEVERITY VARCHAR(5),
                    TABLE_NAME VARCHAR(100),
                    DQ_COLUMN_NAME VARCHAR(100),
                    DQ_COLUMN_VALUE VARCHAR(200),
                    PRIMARY_KEY_COLUMN VARCHAR(100),
                    PRIMARY_KEY_VALUE VARCHAR(200)
                );
            {% endif %}
        {% endset %}
        {{ log("Table Does not Exists, hence creating table", true) }}
        {{ log(create_table_query, true) }}
        {% do run_query(create_table_query) %}
    {% else %}
        {{ log("Table Already Exists, hence skipping Creation", true) }}
    {% endif %}
    {{ log("Inserting New Records", true) }}
    {% do run_query(insert_query) %}
{% endmacro %}
