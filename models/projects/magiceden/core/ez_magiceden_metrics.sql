{{
    config(
        materialized="table",
        snowflake_warehouse="MAGICEDEN",
        database="magiceden",
        schema="core",
        alias="ez_metrics",
    )
}}

WITH magiceden_metrics AS (
    SELECT
        date,
        SUM(daily_trading_volume) AS daily_trading_volume,
        SUM(active_wallets) AS dau, 
        SUM(collections_transacted) AS collections_transacted,
        SUM(total_trades) AS txns,
        SUM(total_platform_fees) AS revenue,
        SUM(total_creator_fees) AS supply_side_fees,
        SUM(total_fees_usd) AS fees
    FROM
        {{ ref('fact_magiceden_metrics_by_chain') }}
    GROUP BY
        date
)
, daily_supply_data as (
    SELECT 
        date
        , 0 as emissions_native
        , premine_unlocks as premine_unlocks_native
        , 0 as burns_native
    FROM {{ ref('fact_magiceden_daily_premine_unlocks') }}
)
, date_spine AS (
    SELECT * 
    FROM {{ ref('dim_date_spine') }}
    WHERE date BETWEEN '2020-03-16' AND TO_DATE(SYSDATE())
)
, market_metrics as (
    {{ get_coingecko_metrics('magic-eden') }}
)
select
    date_spine.date
    , COALESCE(magiceden_metrics.daily_trading_volume, 0) as daily_trading_volume
    , COALESCE(magiceden_metrics.dau, 0) as dau
    , COALESCE(magiceden_metrics.collections_transacted, 0) as collections_transacted
    , COALESCE(magiceden_metrics.txns, 0) as txns
    , COALESCE(magiceden_metrics.revenue, 0) as revenue
    , COALESCE(magiceden_metrics.supply_side_fees, 0) as supply_side_fees
    , COALESCE(magiceden_metrics.fees, 0) as fees
    , COALESCE(magiceden_metrics.supply_side_fees, 0) as nft_royalties

    -- Standardized Metrics

    -- Market Metrics
    , COALESCE(market_metrics.price, 0) as price
    , COALESCE(market_metrics.market_cap, 0) as market_cap
    , COALESCE(market_metrics.fdmc, 0) as fdmc
    , COALESCE(market_metrics.token_volume, 0) as token_volume

    -- NFT Metrics
    , COALESCE(magiceden_metrics.dau, 0) as nft_dau
    , COALESCE(magiceden_metrics.txns, 0) as nft_txns
    , COALESCE(magiceden_metrics.collections_transacted, 0) as nft_collections_transacted
    , COALESCE(magiceden_metrics.daily_trading_volume, 0) as nft_volume

    -- Cash Flow Metrics
    , COALESCE(magiceden_metrics.fees, 0) as gross_protocol_revenue
    , COALESCE(magiceden_metrics.supply_side_fees, 0) as service_cash_flow
    , COALESCE(magiceden_metrics.revenue, 0) as treasury_cash_flow

    -- Turnover Metrics
    , COALESCE(market_metrics.token_turnover_circulating, 0) as token_turnover_circulating
    , COALESCE(market_metrics.token_turnover_fdv, 0) as token_turnover_fdv

    --ME Token Supply Data
    , COALESCE(daily_supply_data.emissions_native, 0) as emissions_native
    , COALESCE(daily_supply_data.premine_unlocks_native, 0) as premine_unlocks_native
    , COALESCE(daily_supply_data.burns_native, 0) as burns_native
    , COALESCE(daily_supply_data.emissions_native, 0) + COALESCE(daily_supply_data.premine_unlocks_native, 0) - COALESCE(daily_supply_data.burns_native, 0) as net_supply_change_native
    --, sum(coalesce(daily_supply_data.emissions_native, 0) + coalesce(daily_supply_data.premine_unlocks_native, 0) - coalesce(daily_supply_data.burns_native, 0)) over (order by daily_supply_data.date) as circulating_supply_native
    , sum(net_supply_change_native) over (order by date_spine.date rows between unbounded preceding and current row) as circulating_supply_native
from date_spine
left join market_metrics on date_spine.date = market_metrics.date
left join magiceden_metrics on date_spine.date = magiceden_metrics.date
left join daily_supply_data on date_spine.date = daily_supply_data.date
where date_spine.date < to_date(sysdate())
order by date_spine.date