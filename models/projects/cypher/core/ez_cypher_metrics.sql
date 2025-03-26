{{
    config(
        materialized="table",
        snowflake_warehouse="CYPHER",
        database="CYPHER",
        schema="core",
        alias="ez_metrics",
    )
}}

with 
    eth as (
        select
            date_trunc('day', date) as day_start,
            sum(transfer_volume) as vol
        from {{ ref("fact_ethereum_stablecoin_transfers") }}
        where
            (
                lower(from_address) in (
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
            group by 1
    ),
    pol as (
        select
            date_trunc('day', date) as day_start,
            sum(transfer_volume) as vol
        from {{ ref("fact_polygon_stablecoin_transfers") }}
        where
            (
                lower(from_address) in (
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
            group by 1
    ),
    arb as (
        select
            date_trunc('day', date) as day_start,
            sum(transfer_volume) as vol
        from {{ ref("fact_arbitrum_stablecoin_transfers") }}
        where
            (
                lower(from_address) in (
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
            group by 1
    ),
    base as (
        select
            date_trunc('day', date) as day_start,
            sum(transfer_volume) as vol
        from {{ ref("fact_base_stablecoin_transfers") }}
        where
            (
                lower(from_address) in (
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
            group by 1
    )

select
    day_start::date as date,
    sum(vol) as transfer_volume
from (
    select * from eth
    union all
    select * from pol
    union all
    select * from arb
    union all
    select * from base
)
group by date
order by date desc