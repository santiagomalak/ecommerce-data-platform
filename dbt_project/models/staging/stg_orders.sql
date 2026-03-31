-- stg_orders: clean and type-cast raw orders
with source as (
    select * from {{ source('raw', 'orders') }}
),

cleaned as (
    select
        order_id,
        customer_id,
        order_status,
        order_purchase_ts::timestamp                               as ordered_at,
        order_approved_ts::timestamp                               as approved_at,
        order_delivered_carrier_ts::timestamp                      as shipped_at,
        order_delivered_customer_ts::timestamp                     as delivered_at,
        order_estimated_delivery_ts::timestamp                     as estimated_delivery_at,

        -- delivery performance
        case
            when order_delivered_customer_ts is not null
             and order_estimated_delivery_ts is not null
            then (order_delivered_customer_ts::timestamp
                  - order_estimated_delivery_ts::timestamp)
            else null
        end                                                        as delivery_delay,

        order_delivered_customer_ts is not null                    as is_delivered
    from source
    where order_id is not null
)

select * from cleaned
