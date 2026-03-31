-- ═══════════════════════════════════════════════════════════
-- 02. RFM Customer Segmentation Analysis
-- ═══════════════════════════════════════════════════════════

-- Segment distribution
select
    segment,
    count(*)                                        as customers,
    round(100.0 * count(*) / sum(count(*)) over (), 2) as pct_customers,
    round(sum(total_revenue)::numeric, 2)           as total_revenue,
    round(avg(avg_order_value)::numeric, 2)         as avg_order_value,
    round(avg(recency_days)::numeric, 1)            as avg_recency_days,
    round(avg(frequency)::numeric, 2)               as avg_frequency,
    round(avg(avg_review_score)::numeric, 2)        as avg_review_score
from marts.mart_rfm_segments
group by 1
order by total_revenue desc;


-- Champions vs Lost: detailed comparison
select
    segment,
    count(*)                            as count,
    round(avg(recency_days)::numeric,1) as avg_recency,
    round(avg(frequency)::numeric,2)    as avg_frequency,
    round(avg(monetary)::numeric,2)     as avg_monetary
from marts.mart_rfm_segments
where segment in ('Champions', 'Lost', 'At Risk', 'Loyal Customers')
group by 1
order by avg_monetary desc;


-- Top 10 customers by revenue
select
    customer_unique_id,
    state,
    total_orders,
    round(total_revenue::numeric, 2)    as total_revenue,
    round(avg_order_value::numeric, 2)  as avg_order_value,
    segment,
    round(avg_review_score::numeric, 2) as avg_review_score
from marts.mart_rfm_segments
order by total_revenue desc
limit 10;
