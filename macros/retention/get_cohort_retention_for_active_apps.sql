{% macro get_cohort_retention_for_active_apps(chain) %}
with apps_to_cover as (
    select
        count(distinct from_address) dau,
        app
    from {{ ref( "fact_" ~ chain ~ "_transactions_v2") }}
    where app is not null
    group by app
), app_list as (
    select app from apps_to_cover where dau > 15
), min_date as (
    select 
        date_trunc('month', min(raw_date)) as first_month,
        t.app
    from {{ ref( "fact_" ~ chain ~ "_transactions_v2") }} t
    left join app_list al on t.app = al.app
    where al.app is not null
    group by 2
), base_table as (
    select
        date_trunc('month', raw_date) as base_month,
        from_address as address,
        t.app
    from {{ ref( "fact_" ~ chain ~ "_transactions_v2") }} t
    left join app_list al on t.app = al.app
    where 
        al.app is not null and
        date_trunc('month', raw_date) < date_trunc('month', sysdate())
    group by 1,2,3
), user_cohorts as (
    select
        address, 
        app,
        min(base_month) as cohort_month
    from
        base_table
    group by 1,2
), cohort_size as (
    select
        cohort_month, 
        app,
        count(distinct(address)) as cohort_size
    from
        user_cohorts
    group by 1,2
), following_months as (
    select
        bt.address, 
        bt.app,
        timestampdiff(month, uc.cohort_month, bt.base_month) as month_number
    from
        base_table as bt
        inner join user_cohorts as uc on bt.address = uc.address and bt.app = uc.app
    where
        bt.base_month > uc.cohort_month
    group by 
        bt.address,
        bt.app,
        month_number
), retention_data as(
    select
        uc.app,
        uc.cohort_month as cohort_month, 
        fm.month_number, 
        count(distinct(fm.address)) as retained_user_count
    from
        following_months as fm
        inner join user_cohorts as uc on fm.address = uc.address and fm.app = uc.app
        left join min_date md on fm.app = md.app
    where
        cohort_month >= md.first_month
    group by 
        uc.app,
        uc.cohort_month, 
        fm.month_number
)
select
    '{{ chain }}' as chain,
    r.app,
    r.cohort_month, 
    c.cohort_size, 
    r.month_number,
    round(r.retained_user_count::numeric / c.cohort_size::numeric , 2) as retention_ratio
from
    retention_data as r
    inner join cohort_size as c on r.cohort_month = c.cohort_month and r.app = c.app
order by
    r.app,
    r.cohort_month, 
    r.month_number
{% endmacro %}