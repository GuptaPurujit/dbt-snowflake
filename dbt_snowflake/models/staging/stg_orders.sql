with source as (
    select
        *
    from
        {{ ref('int_orders__predqm') }}
)

select * from source