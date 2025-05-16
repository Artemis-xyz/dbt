{{
    config(
        materialized = "table",
        snowflake_warehouse = "FLARE",
        database = "FLARE",
        schema = "core",
        alias = "ez_metrics"
    )
}}

with fees as (
    select
        date,
        fees_usd
    from {{ref("fact_flare_fees")}}
)
, txns as (
    select
        date,
        txns
    from {{ref("fact_flare_txns")}}
)
, daus as (
    select
        date,
        dau
    from {{ref("fact_flare_dau")}}
)
, dex_volumes as (
    select
        date,
        daily_volume as dex_volumes
    from {{ref("fact_flare_daily_dex_volumes")}}
)
, defillama_tvl as (
    select
        date,
        tvl
    from {{ref("fact_flare_tvl")}}
)
, daily_supply_data as (
    select
        date,
        gross_emissions_native,
        premine_unlocks_native,
        burns_native,
        net_supply_change_native,
        circulating_supply
    from {{ ref('fact_flare_daily_supply_data') }}
)
, date_spine as (
    select
        ds.date
    from {{ ref('dim_date_spine') }} ds
    where ds.date between (select min(date) from daily_supply_data) and to_date(sysdate())
)
, market_metrics as ({{ get_coingecko_metrics("flare-networks") }})

select
    date_spine.date

    --Old metrics needed for backwards compatibility
    , daus.dau
    , txns.txns
    , fees.fees_usd as fees
    , dex_volumes.dex_volumes

    -- Standardized Metrics

    -- Market Metrics
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Metrics
    , txns.txns AS chain_txns
    , daus.dau AS chain_dau
    , dex_volumes.dex_volumes AS chain_spot_volume
    , defillama_tvl.tvl AS chain_tvl

    -- Cashflow metrics
    , fees.fees_usd AS chain_fees
    , fees.fees_usd AS ecosystem_revenue

    --FLR Token Supply Data
    , daily_supply_data.gross_emissions_native
    , daily_supply_data.premine_unlocks_native
    , daily_supply_data.burns_native
    , daily_supply_data.net_supply_change_native
    , daily_supply_data.circulating_supply as circulating_supply_native

from date_spine
left join fees on date_spine.date = fees.date
left join txns on date_spine.date = txns.date
left join daus on date_spine.date = daus.date 
left join dex_volumes on date_spine.date = dex_volumes.date
left join market_metrics on date_spine.date = market_metrics.date
left join daily_supply_data on date_spine.date = daily_supply_data.date
left join defillama_tvl on date_spine.date = defillama_tvl.date