{{
    config(
        materialized="table",
        snowflake_warehouse="GNOSISPAY",
        database="gnosispay",
        schema="core",
        alias="ez_monthly_retention",
    )
}}

with transfer_data as (
    select 
        date_trunc('day', block_timestamp) as date,
        from_address
    from gnosis_flipside.core.fact_token_transfers
    where lower(contract_address) in (
        lower('0x420CA0f9B9b604cE0fd9C18EF134C705e5Fa3430')
    )
    and lower(to_address) in (
                lower('0x4822521E6135CD2599199c83Ea35179229A172EE'), -- Gnosis Pay aggregator
                lower('0x3d4fd6a1a7a1382ae1d62c3dd7247254a0236847')  -- Gnosis Pay sales address
    )
    and block_timestamp >= date('2024-09-01')
    union all
    select
        date_trunc('day', block_timestamp) as date,
        from_address
    from gnosis_flipside.core.fact_token_transfers
    where lower(contract_address) in (
        lower('0xcB444e90D8198415266c6a2724b7900fb12FC56E')
    )
    and lower(to_address) in (
                lower('0x4822521E6135CD2599199c83Ea35179229A172EE'), -- Gnosis Pay aggregator
                lower('0x3d4fd6a1a7a1382ae1d62c3dd7247254a0236847')  -- Gnosis Pay sales address
    )
    and block_timestamp < date('2024-09-01')
    union all
    select
        date_trunc('day', block_timestamp) as date,
        from_address
    from gnosis_flipside.core.fact_token_transfers
    where lower(contract_address) in (
        lower('0x5Cb9073902F2035222B9749F8fB0c9BFe5527108')
    )
    and lower(to_address) in (
                lower('0x4822521E6135CD2599199c83Ea35179229A172EE'), -- Gnosis Pay aggregator
                lower('0x3d4fd6a1a7a1382ae1d62c3dd7247254a0236847')  -- Gnosis Pay sales address
    )
),
min_date as (
    select 
        date_trunc('month', min(date)) as first_month
    from transfer_data
),
base_table as (
    select
        date_trunc('month', date) as base_month,
        from_address as address
    from transfer_data
    group by 1,2
),
user_cohorts as (
    select
        address, 
        min(base_month) as cohort_month
    from
        base_table
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
retention_data as(
    select
        uc.cohort_month as cohort_month, 
        fm.month_number, 
        count(distinct(fm.address)) as retained_user_count
    from
        following_months as fm
        inner join user_cohorts as uc on fm.address = uc.address
    where
        cohort_month >= (select first_month from min_date)
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