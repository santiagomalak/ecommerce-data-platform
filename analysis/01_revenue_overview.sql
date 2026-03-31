-- ═══════════════════════════════════════════════════════════
-- 01. Revenue Overview — Key business KPIs
-- ═══════════════════════════════════════════════════════════

-- Total revenue, orders and customers
select
    count(distinct order_id)                as total_orders,
    count(distinct customer_id)             as total_customers,
    round(sum(order_revenue)::numeric, 2)   as gross_revenue,
    round(avg(order_revenue)::numeric, 2)   as avg_order_value,
    round(avg(item_count)::numeric, 2)      as avg_items_per_order
from marts.int_orders_enriched
where order_status = 'delivered';


-- Monthly revenue trend
select
    month,
    total_orders,
    unique_customers,
    gross_revenue,
    avg_order_value,
    revenue_growth_pct
from marts.mart_monthly_revenue
order by month;


-- Revenue by Brazilian state
select
    c.state,
    count(distinct o.order_id)              as total_orders,
    round(sum(o.order_revenue)::numeric, 2) as total_revenue,
    round(avg(o.order_revenue)::numeric, 2) as avg_order_value
from marts.int_orders_enriched o
inner join staging.stg_customers c using (customer_id)
where o.order_status = 'delivered'
group by 1
order by total_revenue desc
limit 10;
