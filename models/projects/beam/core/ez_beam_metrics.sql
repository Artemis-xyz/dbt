{{
    config(
        materialized="table",
        snowflake_warehouse="BEAM",
        database="beam",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as (
        select
            date, chain, dau, txns, fees_native
        from {{ ref("fact_beam_fundamental_metrics") }}
    ),
    price_data as ({{ get_coingecko_metrics("beam-2") }}),
    defillama_data as ({{ get_defillama_metrics("beam") }})
select
    fundamental_data.date
    , chain
    , dau
    , txns
    , fees_native
    , fees_native * price as fees
    , fees / txns as avg_txn_fee
    -- Standardized Metrics
    -- Market Metrics
    , price
    , market_cap
    , fdmc
    , token_volume
    -- Chain Metrics
    , dau as chain_dau
    , txns as chain_txns
    -- Cash Flow Metrics
    , fees as chain_fees
    , fees as gross_protocol_revenue
    , fees_native as gross_protocol_revenue_native
    , avg_txn_fee as chain_avg_txn_fee
    -- Crypto Metrics
    , tvl
    , token_turnover_circulating
    , token_turnover_fdv
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
where fundamental_data.date < to_date(sysdate())
