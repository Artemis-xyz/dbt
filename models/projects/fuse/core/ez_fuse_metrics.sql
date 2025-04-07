{{
    config(
        materialized="table",
        snowflake_warehouse="FUSE",
        database="fuse",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as (
        select
            date
            , txns
            , daa as dau
            , gas_usd as fees
            , gas as fees_native
        from {{ ref("fact_fuse_daa_txns_gas_gas_usd") }}
    )
    , github_data as ({{ get_github_metrics("fuse") }})
    , defillama_data as ({{ get_defillama_metrics("fuse") }})
    , price_data as ({{ get_coingecko_metrics("fuse-network-token") }})
select
    fundamental_data.date
    , 'fuse' as chain
    , txns
    , dau
    , fees
    , case when txns > 0 then fees / txns end as avg_txn_fee
    , fees_native
    , dex_volumes
    -- Standardized Metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_turnover_circulating
    , token_turnover_fdv
    , token_volume
    -- Chain Metrics
    , txns as chain_txns
    , dau as chain_dau
    , avg_txn_fee as chain_avg_txn_fee
    , dex_volumes as chain_dex_volumes
    -- Cash Flow Metrics
    , fees as gross_protocol_revenue
    , fees_native as gross_protocol_revenue_native
    -- Crypto Metrics
    , tvl
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
from fundamental_data
left join github_data using (date)
left join defillama_data using (date)
left join price_data using (date)
where fundamental_data.date < to_date(sysdate())