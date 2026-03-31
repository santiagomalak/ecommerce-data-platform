-- int_customer_orders: one row per customer with full order history aggregated
with orders as (
    select * from {{ ref('int_orders_enriched') }}
    where order_status = 'delivered'
),

customers as (
    select * from {{ ref('stg_customers') }}
),

customer_agg as (
    select
        customer_id,
        count(distinct order_id)                            as total_orders,
        sum(order_revenue)                                  as total_revenue,
        avg(order_revenue)                                  as avg_order_value,
        min(ordered_at)                                     as first_order_at,
        max(ordered_at)                                     as last_order_at,
        avg(review_score)                                   as avg_review_score,
        sum(item_count)                                     as total_items_bought,

        -- recency (days since last order relative to dataset max date)
        extract(day from (
            (select max(ordered_at) from orders) - max(ordered_at)
        ))                                                  as recency_days,

        -- frequency
        count(distinct order_id)                            as frequency,

        -- monetary
        sum(order_revenue)                                  as monetary
    from orders
    group by 1
)

select
    c.customer_id,
    c.customer_unique_id,
    c.city,
    c.state,
    a.total_orders,
    a.total_revenue,
    a.avg_order_value,
    a.first_order_at,
    a.last_order_at,
    a.avg_review_score,
    a.total_items_bought,
    a.recency_days,
    a.frequency,
    a.monetary,

    -- RFM scores (1-5 quintiles)
    ntile(5) over (order by a.recency_days desc)    as r_score,
    ntile(5) over (order by a.frequency asc)         as f_score,
    ntile(5) over (order by a.monetary asc)          as m_score
from customers c
inner join customer_agg a using (customer_id)
