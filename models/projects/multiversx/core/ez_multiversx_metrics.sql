{{
    config(
        materialized="table"
        , snowflake_warehouse="MULTIVERSX"
        , database="multiversx"
        , schema="core"
        , alias="ez_metrics"
    )
}}

with
    fundamental_data as (
        select
            date
            , txns
            , daa as dau
            , gas as fees_native
            , gas_usd as fees
            , case when txns > 0 then fees / txns end as avg_txn_fee
        from {{ ref("fact_multiversx_txns") }}
        left join {{ ref("fact_multiversx_daa") }} using (date)
        left join {{ ref("fact_multiversx_gas_gas_usd") }} using (date)
    )
    , github_data as ({{ get_github_metrics("elrond") }})
    , defillama_data as ({{ get_defillama_metrics("elrond") }})
    , price_data as ({{ get_coingecko_metrics("elrond-erd-2") }})

select
    f.date
    , 'multiversx' as chain
    , txns
    , dau
    , fees
    , fees_native
    , avg_txn_fee
    , dex_volumes
    -- Standardized Metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume
    -- Chain Metrics
    , txns as chain_txns
    , dau::number as chain_dau
    , avg_txn_fee as chain_avg_txn_fee
    , dex_volumes as chain_spot_volume
    -- Cash Flow Metrics
    , fees as ecosystem_revenue
    , fees_native as ecosystem_revenue_native
    -- Crypto Metrics
    , tvl
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , token_turnover_circulating
    , token_turnover_fdv
from fundamental_data f
left join github_data using (f.date)
left join defillama_data using (f.date)
left join price_data using (f.date)
where f.date < to_date(sysdate())
