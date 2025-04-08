{{
    config(
        materialized="table",
        snowflake_warehouse="HOLYHELD",
        database="holyheld",
        schema="core",
        alias="ez_monthly_retention",
    )
}}

with
    min_date as (
        select
            date_trunc('month', min(date)) as first_month
        from {{ ref("fact_holyheld_transfers") }}
    ),
    holyheld_table as (
        select
            date_trunc('month', date) as month_start,
            from_address as address
        from {{ ref("fact_holyheld_transfers") }}
    ),
    user_cohorts as (
        select
            address, 
            min(month_start) as cohort_month
        from
            holyheld_table
        group by 1
    ),
    cohort_size as (
        select
            cohort_month, 
            count(distinct(address)) as cohort_size
        from
            user_cohorts
        group by 1
    ),
    following_months as (
        select
            ht.address, 
            timestampdiff(month, uc.cohort_month, ht.month_start) as month_number
        from
            holyheld_table as ht
            inner join user_cohorts as uc on ht.address = uc.address
        where
            ht.month_start > uc.cohort_month
        group by 
            ht.address,
            month_number
    ),
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
    r.cohort_month::date as cohort_month, 
    c.cohort_size, 
    r.month_number,
    round(r.retained_user_count::numeric / c.cohort_size::numeric , 2) as retention_ratio
from
    retention_data as r
    inner join cohort_size as c on r.cohort_month = c.cohort_month
order by
    r.cohort_month, 
    r.month_number