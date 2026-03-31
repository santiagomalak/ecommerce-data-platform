-- mart_rfm_segments: RFM customer segmentation — final analytical table
with base as (
    select * from {{ ref('int_customer_orders') }}
),

rfm as (
    select
        *,
        -- Composite RFM score
        (r_score + f_score + m_score)       as rfm_total,
        concat(r_score, f_score, m_score)   as rfm_cell,

        -- Segment label based on RFM pattern
        case
            when r_score >= 4 and f_score >= 4 and m_score >= 4
                then 'Champions'
            when r_score >= 3 and f_score >= 3 and m_score >= 3
                then 'Loyal Customers'
            when r_score >= 4 and f_score <= 2
                then 'New Customers'
            when r_score >= 3 and f_score >= 1 and m_score >= 2
                then 'Potential Loyalists'
            when r_score = 5 and f_score = 1
                then 'Recent Customers'
            when r_score <= 2 and f_score >= 3 and m_score >= 3
                then 'At Risk'
            when r_score <= 2 and f_score >= 4 and m_score >= 4
                then 'Can\'t Lose Them'
            when r_score <= 2 and f_score <= 2
                then 'Lost'
            else 'Need Attention'
        end                                 as segment
    from base
)

select
    customer_id,
    customer_unique_id,
    city,
    state,
    total_orders,
    total_revenue,
    avg_order_value,
    first_order_at,
    last_order_at,
    recency_days,
    frequency,
    monetary,
    avg_review_score,
    r_score,
    f_score,
    m_score,
    rfm_total,
    rfm_cell,
    segment
from rfm
