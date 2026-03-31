-- mart_seller_performance: seller ranking and KPIs
with items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select
        order_id,
        order_status,
        ordered_at,
        delivered_at,
        delivery_delay,
        review_score,
        sentiment
    from {{ ref('int_orders_enriched') }}
),

sellers as (
    select * from {{ source('raw', 'sellers') }}
),

seller_agg as (
    select
        i.seller_id,
        count(distinct i.order_id)                          as total_orders,
        count(distinct case when o.order_status = 'delivered'
              then i.order_id end)                          as delivered_orders,
        sum(i.price)                                        as gross_revenue,
        avg(i.price)                                        as avg_item_price,
        avg(o.review_score)                                 as avg_review_score,
        sum(case when o.sentiment = 'positive' then 1 else 0 end) as positive_reviews,
        sum(case when o.sentiment = 'negative' then 1 else 0 end) as negative_reviews,
        avg(extract(day from o.delivery_delay))             as avg_delivery_delay_days,
        count(distinct i.product_id)                        as unique_products
    from items i
    inner join orders o using (order_id)
    group by 1
)

select
    a.seller_id,
    upper(s.seller_state)   as state,
    a.total_orders,
    a.delivered_orders,
    round(100.0 * a.delivered_orders / nullif(a.total_orders, 0), 2) as delivery_rate,
    a.gross_revenue,
    a.avg_item_price,
    a.avg_review_score,
    a.positive_reviews,
    a.negative_reviews,
    a.avg_delivery_delay_days,
    a.unique_products,
    -- Seller tier
    case
        when a.gross_revenue >= 50000 and a.avg_review_score >= 4 then 'Platinum'
        when a.gross_revenue >= 20000 and a.avg_review_score >= 3.5 then 'Gold'
        when a.gross_revenue >= 5000  then 'Silver'
        else 'Bronze'
    end as seller_tier
from seller_agg a
inner join sellers s using (seller_id)
order by a.gross_revenue desc
