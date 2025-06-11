{{
    config(
        materialized="table",
        snowflake_warehouse="ZCASH",
        database="zcash",
        schema="core",
        alias="ez_metrics",
        enabled=false,
    )
}}

with
    fundamental_data as (
        select
            date
            , txns
            , gas_usd as fees
            , gas as fees_native
        from {{ ref("fact_zcash_gas_gas_usd_txns") }}
    )
    , github_data as ({{ get_github_metrics("zcash") }})
    , price_data as ({{ get_coingecko_metrics('zcash') }})

select
    f.date

    -- Standardized Metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume
    --chain metrics
    , txns as chain_txns
    , fees as ecosystem_revenue
    , fees_native as ecosystem_revenue_native
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , token_turnover_circulating
    , token_turnover_fdv
from fundamental_data f
left join github_data using (date)
left join price_data on f.date = price_data.date
where f.date < to_date(sysdate())
