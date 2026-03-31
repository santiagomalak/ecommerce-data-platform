-- ═══════════════════════════════════════════════════════════
-- 03. Cohort Retention Analysis
-- ═══════════════════════════════════════════════════════════

-- Full retention matrix (pivot-style)
select
    cohort_month,
    cohort_size,
    max(case when month_index = 0  then retention_rate end) as "month_0",
    max(case when month_index = 1  then retention_rate end) as "month_1",
    max(case when month_index = 2  then retention_rate end) as "month_2",
    max(case when month_index = 3  then retention_rate end) as "month_3",
    max(case when month_index = 6  then retention_rate end) as "month_6",
    max(case when month_index = 12 then retention_rate end) as "month_12"
from marts.mart_cohort_retention
group by cohort_month, cohort_size
order by cohort_month;


-- Average retention by month index
select
    month_index,
    round(avg(retention_rate)::numeric, 2)  as avg_retention_rate,
    count(distinct cohort_month)            as cohorts_with_data
from marts.mart_cohort_retention
group by 1
order by 1
limit 13;


-- Best and worst performing cohorts (month_1 retention)
select
    cohort_month,
    cohort_size,
    retention_rate as month_1_retention
from marts.mart_cohort_retention
where month_index = 1
order by retention_rate desc
limit 10;
