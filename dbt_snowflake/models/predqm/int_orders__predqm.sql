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

select * from updated_source