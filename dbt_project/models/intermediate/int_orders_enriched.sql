-- int_orders_enriched: orders joined with items, payments and reviews
with orders as (
    select * from {{ ref('stg_orders') }}
),

items_agg as (
    select
        order_id,
        count(*)                        as item_count,
        sum(price)                      as items_revenue,
        sum(freight_value)              as freight_revenue,
        sum(total_value)                as order_revenue
    from {{ ref('stg_order_items') }}
    group by 1
),

payments_agg as (
    select
        order_id,
        sum(payment_value)              as total_paid,
        max(payment_type)               as primary_payment_type,
        max(payment_installments)       as max_installments
    from {{ ref('stg_payments') }}
    group by 1
),

reviews as (
    select
        order_id,
        review_score,
        sentiment
    from {{ ref('stg_reviews') }}
)

select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.ordered_at,
    o.approved_at,
    o.shipped_at,
    o.delivered_at,
    o.estimated_delivery_at,
    o.delivery_delay,
    o.is_delivered,

    -- items
    coalesce(i.item_count, 0)           as item_count,
    coalesce(i.items_revenue, 0)        as items_revenue,
    coalesce(i.freight_revenue, 0)      as freight_revenue,
    coalesce(i.order_revenue, 0)        as order_revenue,

    -- payments
    coalesce(p.total_paid, 0)           as total_paid,
    p.primary_payment_type,
    p.max_installments,

    -- reviews
    r.review_score,
    r.sentiment,

    -- time dimensions
    date_trunc('month', o.ordered_at)   as order_month,
    date_trunc('week',  o.ordered_at)   as order_week,
    extract(year  from o.ordered_at)    as order_year,
    extract(month from o.ordered_at)    as order_month_num

from orders o
left join items_agg    i using (order_id)
left join payments_agg p using (order_id)
left join reviews      r using (order_id)
