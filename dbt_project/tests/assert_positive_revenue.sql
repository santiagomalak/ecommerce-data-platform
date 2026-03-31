-- Test: no negative revenue in orders
select order_id
from {{ ref('int_orders_enriched') }}
where order_revenue < 0
