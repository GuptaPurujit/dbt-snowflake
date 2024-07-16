{% macro generate_stg_query(table_name, source) %}
    {% set config_query %}
        SELECT
            'trim' AS Std_C,
            'o_orderkey' AS Col_name,
            'ORDER_KEY' AS Targ_col_name
        FROM
            "PROCESSED"."CONFIG"."DATASET_DETAILS"
        WHERE
            table_name = '{{ table_name }}'
    {% endset %}
    {{ log("Running Query: " ~ config_query, info=True) }}

    {% set results = run_query(config_query) %}

    {% set transformations = [] %}
 
    {% for row in results %}
        {% if row[0] == 'trim' %}
            {% set transformation = "trim(" ~ row[1] ~ ") AS " ~ row[2] %}
        {% elif row[0] == 'lowercase' %}
            {% set transformation = "lower(" ~ row[1] ~ ") AS " ~ row[2] %}
        {% elif row[0] == 'uppercase' %}
            {% set transformation = "upper(" ~ row[1] ~ ") AS " ~ row[2] %}
        {% else %}
            {% set transformation = row[1] ~ " AS " ~ row[2] %}
        {% endif %}
 
        {% set transformations = transformations.append(transformation) %}
    {% endfor %}

    {{ log(transformations, info=True) }}

    SELECT
        {{ transformations | join(', ') }}
    FROM {{ source }}
{% endmacro %}