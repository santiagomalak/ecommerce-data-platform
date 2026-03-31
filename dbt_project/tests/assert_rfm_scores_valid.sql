-- Test: RFM scores must be between 1 and 5
select customer_id
from {{ ref('mart_rfm_segments') }}
where r_score not between 1 and 5
   or f_score not between 1 and 5
   or m_score not between 1 and 5
