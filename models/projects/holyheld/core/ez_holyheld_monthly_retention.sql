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
    pol as (
        select
            from_address,
            'pol' as chain,
            date
        from {{ ref("fact_polygon_stablecoin_transfers") }}
        where 
            lower(to_address) = lower('0x0146dca5eD7fAc1Dd53A2791089E109645732E1c') 
            and lower(contract_address) = lower('0x3c499c542cef5e3811e1192ce70d8cc03d5c3359')
            and date >= '2023-03-13'
    ),
    avax as (
        select
            from_address,
            'avax' as chain,
            date
        from {{ ref("fact_avalanche_stablecoin_transfers") }}
        where 
            lower(to_address) = lower('0x0146dca5eD7fAc1Dd53A2791089E109645732E1c') 
            and (lower(contract_address) = lower('0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e') 
                or lower(contract_address) = lower('0xC891EB4cbdEFf6e073e859e987815Ed1505c2ACD'))
            and date >= '2023-03-14'
    ),
    eth as (
        select
            from_address,
            'eth' as chain,
            date
        from {{ ref("fact_ethereum_stablecoin_transfers") }}
        where 
            lower(to_address) = lower('0x0146dca5eD7fAc1Dd53A2791089E109645732E1c') 
            and (lower(contract_address) = lower('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48') 
                or lower(contract_address) = lower('0xdAC17F958D2ee523a2206206994597C13D831ec7') 
                or lower(contract_address) = lower('0x1aBaEA1f7C830bD89Acc67eC4af516284b1bC33c'))
            and date >= '2023-03-14'
    ),
    op as (
        select
            from_address,
            'op' as chain,
            date
        from {{ ref("fact_optimism_stablecoin_transfers") }}
        where 
            lower(to_address) = lower('0x0146dca5eD7fAc1Dd53A2791089E109645732E1c') 
            and lower(contract_address) = lower('0x0b2c639c533813f4aa9d7837caf62653d097ff85')
            and date >= '2023-03-14'
    ),
    arb as (
        select
            from_address,
            'arb' as chain,
            date
        from {{ ref("fact_arbitrum_stablecoin_transfers") }}
        where 
            lower(to_address) = lower('0x0146dca5eD7fAc1Dd53A2791089E109645732E1c') 
            and lower(contract_address) = lower('0xaf88d065e77c8cc2239327c5edb3a432268e5831')
            and date >= '2023-03-14'
    ),
    base as (
        select
            from_address,
            'base' as chain,
            date
        from {{ ref("fact_base_stablecoin_transfers") }}
        where 
            lower(to_address) = lower('0x0146dca5eD7fAc1Dd53A2791089E109645732E1c') 
            and lower(contract_address) = lower('0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913')
            and date >= '2023-03-14'
    ),
    bsc as (
        select
            from_address,
            'bsc' as chain,
            date
        from {{ ref("fact_bsc_stablecoin_transfers") }}
        where 
            lower(to_address) = lower('0x0146dca5eD7fAc1Dd53A2791089E109645732E1c') 
            and lower(contract_address) = lower('0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d')
            and date >= '2024-08-01'
    ),
    all_table as (
        select * from pol
        union all
        select * from avax
        union all
        select * from eth
        union all
        select * from op
        union all
        select * from arb
        union all
        select * from base
        union all
        select * from bsc
    ),
    min_date as (
        select
            date_trunc('month', min(date)) as first_month
        from all_table
    ),
    holyheld_table as (
        select
            date_trunc('month', date) as month_start,
            from_address as address
        from all_table
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