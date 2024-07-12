{% macro create_or_update_table(database=target.database, schema=target.schema, insert_flag=True, table=None, query=None) %}
    {% set table_exists = adapter.get_relation(database=database, schema=schema, identifier=table) is not none %}
    {% if insert_flag %}
        {% set insert_query %}
            INSERT INTO {{ database }}.{{ schema }}.{{ table }}
            ({{ query }})
        {% endset %}
    {% else %}
        {% set insert_query %}
            {{ query }}
        {% endset %}
    {% endif %}

    {% if not table_exists %}
        {% set create_table_query %}
            {% if table == 'dqm_summary' %}
                CREATE TRANSIENT TABLE IF NOT EXISTS {{ database }}.{{ schema }}.{{ table }} (
                    INVOCATION_ID VARCHAR(36),
                    TIMESTAMP TIMESTAMP_NTZ(9),
                    CHECK_TYPE VARCHAR(100),
                    SEVERITY VARCHAR(5),
                    CRITICALITY VARCHAR(2),
                    TABLE_NAME VARCHAR(100),
                    DQ_COLUMN_NAME VARCHAR(100),
                    RECORD_COUNT NUMBER(38,0),
                    FAILED_RECORD_COUNT NUMBER(38,0),
                    STATUS VARCHAR(10)
                );
            {% elif table == 'dqm_details' %}
                CREATE TRANSIENT TABLE IF NOT EXISTS {{ database }}.{{ schema }}.{{ table }} (
                    INVOCATION_ID VARCHAR(36),
                    TIMESTAMP TIMESTAMP_NTZ(9),
                    CHECK_TYPE VARCHAR(100),
                    SEVERITY VARCHAR(5),
                    CRITICALITY VARCHAR(2),
                    TABLE_NAME VARCHAR(100),
                    DQ_COLUMN_NAME VARCHAR(100),
                    DQ_COLUMN_VALUE VARCHAR(200),
                    PRIMARY_KEY_COLUMN VARCHAR(100),
                    PRIMARY_KEY_VALUE VARCHAR(200)
                );
            {% elif table == 'process_summary' %}
                CREATE TRANSIENT TABLE IF NOT EXISTS {{ database }}.{{ schema }}.{{ table }} (
                    INVOCATION_ID VARCHAR(36),
                    STEP_NAME VARCHAR(100),
                    STATUS VARCHAR(100),
                    START_TIME TIMESTAMP_NTZ(9),
                    END_TIME TIMESTAMP_NTZ(9)
                );
            {% endif %}
        {% endset %}
        {{ log("Table Does not Exists, hence creating table", true) }}
        {% do run_query(create_table_query) %}
    {% else %}
        {{ log("Table Already Exists, hence skipping Creation", true) }}
    {% endif %}
    {{ log("Inserting New Records", true) }}
    {% do run_query(insert_query) %}
{% endmacro %}
