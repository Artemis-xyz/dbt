{% macro get_cohort_retention(chain) %}
    with 
min_date as (
    select 
        date_trunc('month', min(raw_date)) as first_month
    from {{ ref( "fact_" ~ chain ~ "_transactions_v2") }}
),
base_table as(
    select
        date_trunc('month', raw_date) as base_month,
        from_address as address
    from {{ ref( "fact_" ~ chain ~ "_transactions_v2") }}
    where 
        date_trunc('month', raw_date) < date_trunc('month', sysdate())
    group by 1,2
),
-- from user_cohorts onward, should be able to reuse ctes on any chain 

-- grab each users first action timestamp on chain
user_cohorts as (
    select
        address, 
        min(base_month) as cohort_month
    from
        base_table
    group by 1
),

--compute cohort size per distinct cohort period
cohort_size as (
    select
        cohort_month, 
        count(distinct(address)) as cohort_size
    from
        user_cohorts
    group by 1
),

-- determine if/ when users came back for another interaction based on period number
following_months as (
    select
        bt.address, 
        timestampdiff(month, uc.cohort_month, bt.base_month) as month_number
    from
        base_table as bt
        inner join user_cohorts as uc on bt.address = uc.address
    where
        bt.base_month > uc.cohort_month
    group by 
        bt.address,
        month_number
),
--aggregate and calcualte the retained user amount per time period per cohort
retention_data as(
    select
        uc.cohort_month as cohort_month, 
        fm.month_number, 
        count(distinct(fm.address)) as retained_user_count
    from
        following_months as fm
        inner join user_cohorts as uc on fm.address = uc.address
    where
        cohort_month >= (select first_month from min_date) -- Grab data from the beginning of the month 24 months ago
    group by 
        uc.cohort_month, 
        fm.month_number
)
select
    '{{ chain }}' as chain,
    r.cohort_month, 
    c.cohort_size, 
    r.month_number,
    round(r.retained_user_count::numeric / c.cohort_size::numeric , 2) as retention_ratio
from
    retention_data as r
    inner join cohort_size as c on r.cohort_month = c.cohort_month
order by
    r.cohort_month, 
    r.month_number

{% endmacro %}