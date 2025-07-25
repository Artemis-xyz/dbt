{{
    config(
        materialized = "incremental",
        snowflake_warehouse = "FLARE",
        database = "FLARE",
        schema = "core",
        alias = "ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

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
, dune_dex_volumes as (
    select
        date,
        daily_volume as dex_volumes,
        daily_volume_adjusted as adjusted_dex_volumes
    from {{ref("fact_flare_daily_dex_volumes")}}
)
, defillama_tvl as (
    select
        date,
        tvl
    from {{ref("fact_flare_tvl")}}
)
, issued_supply_metrics as (
    select 
        date,
        daily_inflation,
        max_supply_to_date,
        burns_daily,
        total_supply_to_date,
        issued_supply,
        total_unlocks_daily,
        circulating_supply
    from {{ ref('fact_flare_issued_supply_metrics') }}
)

, date_spine as (
    select
        ds.date
    from {{ ref('dim_date_spine') }} ds
    where ds.date between (select min(date) from issued_supply_metrics) and to_date(sysdate())
)
, market_metrics as ({{ get_coingecko_metrics("flare-networks") }})

select
    date_spine.date

    -- Standardized Metrics
    -- Market Metrics
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume
    -- Usage Metrics
    , daus.dau AS chain_dau
    , txns.txns AS chain_txns
    , dune_dex_volumes.dex_volumes AS chain_spot_volume
    , defillama_tvl.tvl AS chain_tvl

    -- Cashflow Metrics
    , fees.fees_usd AS chain_fees

    -- Issued Supply Metrics
    , issued_supply_metrics.daily_inflation as gross_emissions_native
    , issued_supply_metrics.max_supply_to_date as max_supply_native
    , issued_supply_metrics.burns_daily as burns_native
    , issued_supply_metrics.total_supply_to_date as total_supply_native
    , issued_supply_metrics.issued_supply as issued_supply_native
    , issued_supply_metrics.total_unlocks_daily as premine_unlocks_native
    , issued_supply_metrics.circulating_supply as circulating_supply_native

    -- Financial Statement Metrics
    , fees.fees_usd AS revenue

    --Token Turnover Data
    , market_metrics.token_turnover_fdv
    , market_metrics.token_turnover_circulating

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from date_spine
left join fees on date_spine.date = fees.date
left join txns on date_spine.date = txns.date
left join daus on date_spine.date = daus.date 
left join dune_dex_volumes on date_spine.date = dune_dex_volumes.date
left join market_metrics on date_spine.date = market_metrics.date
left join defillama_tvl on date_spine.date = defillama_tvl.date
left join issued_supply_metrics on date_spine.date = issued_supply_metrics.date
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())