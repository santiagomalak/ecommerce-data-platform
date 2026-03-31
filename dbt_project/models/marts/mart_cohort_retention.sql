-- mart_cohort_retention: monthly cohort retention analysis
with orders as (
    select
        customer_id,
        date_trunc('month', ordered_at) as order_month
    from {{ ref('int_orders_enriched') }}
    where order_status = 'delivered'
      and ordered_at is not null
),

-- First purchase month per customer = cohort
cohorts as (
    select
        customer_id,
        min(order_month) as cohort_month
    from orders
    group by 1
),

-- All activity months per customer
customer_activity as (
    select
        o.customer_id,
        c.cohort_month,
        o.order_month,
        -- Month index: 0 = acquisition month
        extract(year  from age(o.order_month, c.cohort_month)) * 12
        + extract(month from age(o.order_month, c.cohort_month)) as month_index
    from orders o
    inner join cohorts c using (customer_id)
),

-- Cohort size (number of new customers per cohort month)
cohort_sizes as (
    select
        cohort_month,
        count(distinct customer_id) as cohort_size
    from cohorts
    group by 1
),

-- Active customers per cohort per month index
retention as (
    select
        cohort_month,
        month_index,
        count(distinct customer_id) as active_customers
    from customer_activity
    group by 1, 2
)

select
    r.cohort_month,
    r.month_index,
    r.active_customers,
    cs.cohort_size,
    round(100.0 * r.active_customers / cs.cohort_size, 2) as retention_rate
from retention r
inner join cohort_sizes cs using (cohort_month)
order by cohort_month, month_index
