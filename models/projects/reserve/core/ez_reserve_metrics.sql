{{
    config(
        materialized="incremental",
        database = 'RESERVE',
        schema = 'core',
        snowflake_warehouse = 'RESERVE',
        alias = 'ez_metrics',
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with date_spine as (
    select date
    from {{ ref("dim_date_spine") }}
    where date between '2021-10-01' and to_date(sysdate())
)
, dau as (
    select
        date
        , coalesce(dau, 0) as dau
    from {{ ref("fact_reserve_dau") }}
)
, tvl as (
    select
        date
        , tvl as tvl
    from {{ ref("fact_reserve_tvl") }}
)
, market_metrics as (
    {{ get_coingecko_metrics('reserve-rights-token') }}
)
, rtoken_market_cap as (
    select
        date
        , rtokens_mc
    from {{ ref("fact_reserve_rtoken_market_cap") }}
)
, reserve_fundamental_data as (
    select
        ds.date
        , coalesce(dau, 0) as dau
        , coalesce(tvl, 0) as tvl
        , coalesce(rtokens_mc, 0) as rtokens_mc

        -- Fill forward market cap and price
        , last_value(market_data.market_cap ignore nulls) over (order by ds.date) as market_cap_filled
        , last_value(market_data.price ignore nulls) over (order by ds.date) as price_filled

    from date_spine ds
    left join tvl using (date)
    left join dau using (date)
    left join rtoken_market_cap using (date)
)

, protocol_revenue as (
    select
        date
        , coalesce(sum(ecosystem_revenue), 0) as revenue
    from {{ ref("fact_reserve_protocol_revenue") }}
    group by date
)

, supply_data as (
    select
        date
        , coalesce(premine_unlocks_native, 0) as premine_unlocks_native
        , coalesce(gross_emissions_native, 0) as gross_emissions_native
        , coalesce(burns_native, 0) as burns_native
        , coalesce(net_supply_change_native, 0) as net_supply_change_native
        , coalesce(circulating_supply_native, 0) as circulating_supply_native
    from {{ ref("fact_reserve_supply_data") }}
)

select
    reserve_fundamental_data.date
    , 'reserve' as artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Data
    , reserve_fundamental_data.dau as dau
    , reserve_fundamental_data.tvl as tvl

    -- Financial Statements
    , protocol_revenue.revenue as revenue
    
    -- Turnover Metrics
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- Supply Metrics
    , supply_data.premine_unlocks_native
    , supply_data.gross_emissions_native
    , supply_data.burns_native
    , supply_data.net_supply_change_native
    , circulating_supply_native

    -- Bespoke Metrics
    , reserve_fundamental_data.rtokens_mc as rtokens_mc

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from reserve_fundamental_data
left join protocol_revenue using (date)
left join supply_data using (date)
left join market_metrics using (date)
where true
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())