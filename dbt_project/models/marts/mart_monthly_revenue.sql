-- mart_monthly_revenue: monthly revenue, orders and KPIs time series
with orders as (
    select * from {{ ref('int_orders_enriched') }}
    where order_status = 'delivered'
      and ordered_at is not null
)

select
    order_month                                             as month,
    order_year                                             as year,
    count(distinct order_id)                               as total_orders,
    count(distinct customer_id)                            as unique_customers,
    sum(order_revenue)                                     as gross_revenue,
    sum(freight_revenue)                                   as freight_revenue,
    sum(items_revenue)                                     as items_revenue,
    avg(order_revenue)                                     as avg_order_value,
    avg(item_count)                                        as avg_items_per_order,
    avg(review_score)                                      as avg_review_score,
    sum(case when sentiment = 'positive' then 1 else 0 end)  as positive_reviews,
    sum(case when sentiment = 'negative' then 1 else 0 end)  as negative_reviews,

    -- Month-over-month growth (using LAG)
    lag(sum(order_revenue)) over (order by order_month)    as prev_month_revenue,
    round(
        100.0 * (sum(order_revenue) - lag(sum(order_revenue)) over (order by order_month))
             / nullif(lag(sum(order_revenue)) over (order by order_month), 0),
        2
    )                                                       as revenue_growth_pct

from orders
group by order_month, order_year
order by order_month
