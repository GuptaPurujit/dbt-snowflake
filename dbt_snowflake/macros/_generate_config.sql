{% macro safe_bool(value) %}
    {% if value is string %}
        {{ return(value.lower() == 'true' or value == '1') }}
    {% elif value is number %}
        {{ return(value != 0) }}
    {% else %}
        {{ return(value) }}
    {% endif %}
{% endmacro %}

{% macro fetch_config_from_audit_system(table_name) %}
    {% set config_query %}
        SELECT
            load_type,
            unique_key,
            transient_table_flag,
            variable_schema_flag
        FROM
            "PROCESSED"."CONFIG"."DATASET_DETAILS"
        WHERE
            table_name = '{{ table_name }}'
    {% endset %}
    {{ log("Running Query: " ~ config_query, info=True) }}

    {% set results = run_query(config_query) %}
    {% set config_dict = {'materialized': 'view'} %}  -- Set default to 'view' to ensure some config always exists

    {% if results and results.column_names and results.rows %}
        {% set row = results.rows[0] %}
        {% set load_type = row[0].lower() %}
        {% set unique_key = row[1] %}
        {% set transient_flag = safe_bool(row[2]) %}
        {% set variable_schema_flag = safe_bool(row[3]) %}
        
        {% do config_dict.update({'materialized': 'table' if load_type == 'full' else 'incremental'}) %}
        {% do config_dict.update({'unique_key': unique_key|trim, 'transient': transient_flag}) %}
        
        {% if load_type == 'incremental' %}
            {% do config_dict.update({'on_schema_change': 'sync_all_columns' if variable_schema_flag else 'fail'}) %}
        {% endif %}
    {% else %}
        {{ log("Warning: No results found for model " ~ table_name ~ ", defaulting to 'table'", info=True) }}
    {% endif %}

    {{ log("Final config dict: " ~ config_dict, info=True) }}
    {{ return(config_dict) }}
{% endmacro %}