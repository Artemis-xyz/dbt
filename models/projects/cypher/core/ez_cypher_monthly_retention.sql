{{
    config(
        materialized="table",
        snowflake_warehouse="CYPHER",
        database="CYPHER",
        schema="core",
        alias="ez_monthly_retention",
    )
}}

with 
    eth as (
        select
            from_address,
            date
        from {{ ref("fact_ethereum_stablecoin_transfers") }}
        where
            (
                lower(to_address) in (
                    lower('0xcfdAb76b36B33dA54c08314A9F265588B67170dc'),
                    lower('0xcCCd218A58B53C67fC17D8C87Cb90d83614e35fD'),
                    lower('0x3cb7367aC1E6a439dA1f1717f8055f02E3C9d56e'),
                    lower('0x154E719D0513B015194b8C6977e524508bb35276')
                )
                and
                lower(contract_address) in (
                    lower('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'),
                    lower('0xdac17f958d2ee523a2206206994597c13d831ec7')
                )
                and
                date >= '2024-01-01'
            )
    ),
    pol as (
        select
            from_address,
            date
        from {{ ref("fact_polygon_stablecoin_transfers") }}
        where
            (
                lower(to_address) in (
                    lower('0xcfdAb76b36B33dA54c08314A9F265588B67170dc'),
                    lower('0xcCCd218A58B53C67fC17D8C87Cb90d83614e35fD'),
                    lower('0x3cb7367aC1E6a439dA1f1717f8055f02E3C9d56e'),
                    lower('0x154E719D0513B015194b8C6977e524508bb35276')
                )
                and
                lower(contract_address) in (
                    lower('0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359'),
                    lower('0xc2132d05d31c914a87c6611c10748aeb04b58e8f')
                )
                and
                date >= '2024-01-01'
            )
    ),
    arb as (
        select
            from_address,
            date
        from {{ ref("fact_arbitrum_stablecoin_transfers") }}
        where
            (
                lower(to_address) in (
                    lower('0xcfdAb76b36B33dA54c08314A9F265588B67170dc'),
                    lower('0xcCCd218A58B53C67fC17D8C87Cb90d83614e35fD'),
                    lower('0x3cb7367aC1E6a439dA1f1717f8055f02E3C9d56e'),
                    lower('0x154E719D0513B015194b8C6977e524508bb35276')
                )
                and
                lower(contract_address) in (
                    lower('0xaf88d065e77c8cC2239327C5EDb3A432268e5831'),
                    lower('0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9')
                )
                and
                date >= '2024-01-01'
            )
    ),
    base as (
        select
            from_address,
            date
        from {{ ref("fact_base_stablecoin_transfers") }}
        where
            (
                lower(to_address) in (
                    lower('0xcfdAb76b36B33dA54c08314A9F265588B67170dc'),
                    lower('0xcCCd218A58B53C67fC17D8C87Cb90d83614e35fD'),
                    lower('0x3cb7367aC1E6a439dA1f1717f8055f02E3C9d56e'),
                    lower('0x154E719D0513B015194b8C6977e524508bb35276')
                )
                and
                lower(contract_address) = lower('0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913')
                and
                date >= '2024-01-01'
            )
    ),
    cypher as (
        select * from eth
        union all
        select * from pol
        union all
        select * from arb
        union all
        select * from base
    ),
    min_date as (
        select 
            date_trunc('month', min(date)) as first_month
        from cypher
    ),
    base_table as (
        select
            date_trunc('month', date) as base_month,
            from_address as address
        from cypher
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