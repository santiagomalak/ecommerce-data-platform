-- ═══════════════════════════════════════════════════════════
-- 04. Seller Performance & Quality Analysis
-- ═══════════════════════════════════════════════════════════

-- Tier distribution
select
    seller_tier,
    count(*)                                    as sellers,
    round(sum(gross_revenue)::numeric, 2)       as total_revenue,
    round(avg(avg_review_score)::numeric, 2)    as avg_review_score,
    round(avg(delivery_rate)::numeric, 2)       as avg_delivery_rate,
    round(avg(avg_delivery_delay_days)::numeric, 1) as avg_delay_days
from marts.mart_seller_performance
group by 1
order by total_revenue desc;


-- Top 10 sellers by revenue
select
    seller_id,
    state,
    seller_tier,
    total_orders,
    round(gross_revenue::numeric, 2)        as gross_revenue,
    round(avg_review_score::numeric, 2)     as avg_review_score,
    round(delivery_rate::numeric, 2)        as delivery_rate
from marts.mart_seller_performance
order by gross_revenue desc
limit 10;


-- Sellers with high revenue but low reviews (at-risk)
select
    seller_id,
    seller_tier,
    round(gross_revenue::numeric, 2)    as gross_revenue,
    avg_review_score,
    negative_reviews,
    round(avg_delivery_delay_days::numeric, 1) as avg_delay_days
from marts.mart_seller_performance
where gross_revenue > 10000
  and avg_review_score < 3.5
order by gross_revenue desc;
