{{ config (
        fromjson(var('config_dict'))
    )
}}

with source as (
    select
        *
    from
        {{ source('raw', 'orders') }}
),

updated_source as (
    select
        *,
        case when o_custkey = '1481' then null
        else 1
        end as dq_col
    from
        source
)

{% if execute %}
    {{ generate_stg_query('int_orders__predqm.sql', source('raw', 'orders')) }}
{% endif %}