with source as (
    select * from {{ source('jaffle_shop', 'raworders') }}
),
renamed as (
    select
        id as order_id,
        customer as customer,
        ordered_at as ordered_at,
        store_id as store_id

    from source
)
select * from renamed
