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
        full_refresh=false,
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
        , dau
    from {{ ref("fact_reserve_dau") }}
)
, tvl as (
    select
        date
        , tvl
    from {{ ref("fact_reserve_tvl") }}
)
, market_data as (
    {{ get_coingecko_metrics('reserve-rights-token') }}
)
, rtoken_market_cap as (
    select
        date
        , rtokens_mc
    from {{ ref("fact_reserve_rtoken_market_cap") }}
)
, forward_filled_data as (
    select
        ds.date
        , dau
        , tvl
        , rtokens_mc
        , price
        , market_cap
        , fdmc
        , token_volume
        
        , token_turnover_circulating
        , token_turnover_fdv

        -- Fill forward market cap and price
        , last_value(market_data.market_cap ignore nulls) over (order by ds.date) as market_cap_filled
        , last_value(market_data.price ignore nulls) over (order by ds.date) as price_filled

    from date_spine ds
    left join tvl using (date)
    left join dau using (date)
    left join market_data using (date)
    left join rtoken_market_cap using (date)
)

, protocol_revenue as (
    select
        date
        , sum(ecosystem_revenue) as ecosystem_revenue
    from {{ ref("fact_reserve_protocol_revenue") }}
    group by date
)

, supply_data as (
    select
        date
        , premine_unlocks_native
        , gross_emissions_native
        , burns_native
        , net_supply_change_native
        , circulating_supply_native
    from {{ ref("fact_reserve_supply_data") }}
)

select
    date
    , dau

    -- Standardized Metrics
    , coalesce(ecosystem_revenue, 0) as fees

    -- Token Metrics
    , coalesce(price, 0) as price
    , coalesce(market_cap, 0) as market_cap
    , coalesce(fdmc, 0) as fdmc
    , coalesce(token_volume, 0) as token_volume

    -- Stablecoin Metrics
    , coalesce(dau, 0) as stablecoin_dau

    -- Crypto Metrics
    , coalesce(tvl, 0) as tvl
    , coalesce(rtokens_mc, 0) as rtokens_mc
    -- Turnover Metrics
    , coalesce(token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(token_turnover_fdv, 0) as token_turnover_fdv

    -- Supply Metrics
    , premine_unlocks_native
    , gross_emissions_native
    , burns_native
    , net_supply_change_native
    , circulating_supply_native

    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
from forward_filled_data
left join protocol_revenue using (date)
left join supply_data using (date)
where true
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())