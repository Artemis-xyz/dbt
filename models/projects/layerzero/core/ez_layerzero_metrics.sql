{{
    config(
        materialized="view",
        snowflake_warehouse="LAYERZERO",
        database="layerzero",
        schema="core",
        alias="ez_metrics",
    )
}}

with bridge_volume as (
    SELECT
        date,
        avg(bridge_volume) as bridge_volume
    FROM {{ ref('fact_layerzero_bridge_volume_all_chains') }}
    GROUP BY 1
)
, bridge_metrics as (
    SELECT
        date
        , sum(bridge_dau) as bridge_dau
        , sum(fees) as fees
        , sum(bridge_txns) as bridge_txns
    FROM {{ ref('ez_layerzero_metrics_by_chain') }}
    GROUP BY 1
)
, daily_supply_data as (
    SELECT 
        date
        , 0 as emissions_native
        , premine_unlocks as premine_unlocks_native
        , 0 as burns_native
    FROM {{ ref('fact_layerzero_daily_premine_unlocks') }}
)
, date_spine as (
    SELECT * 
    FROM {{ ref('dim_date_spine') }}
    WHERE date BETWEEN '2020-03-16' AND TO_DATE(SYSDATE())
)
, market_metrics as (
    {{ get_coingecko_metrics("layerzero") }}
)

select
    date_spine.date

    , coalesce(bridge_metrics.fees, 0) as fees
    , coalesce(bridge_metrics.bridge_dau, 0) as bridge_daa

    -- Standardized Metrics

    -- Token Metrics
    , coalesce(market_metrics.price, 0) as price
    , coalesce(market_metrics.market_cap, 0) as market_cap
    , coalesce(market_metrics.fdmc, 0) as fdmc
    , coalesce(market_metrics.token_volume, 0) as token_volume

    -- Usage Metrics
    , coalesce(bridge_metrics.bridge_dau, 0) as bridge_dau
    , coalesce(bridge_metrics.bridge_txns, 0) as bridge_txns
    , coalesce(bridge_volume.bridge_volume, 0) as bridge_volume

    -- Cash Flow Metrics
    , coalesce(bridge_metrics.fees, 0) as bridge_fees
    , coalesce(bridge_metrics.fees, 0) as ecosystem_revenue

    -- Turnover Metrics
    , coalesce(market_metrics.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(market_metrics.token_turnover_fdv, 0) as token_turnover_fdv

    -- ZRO Token Supply Data
    , coalesce(daily_supply_data.emissions_native, 0) as emissions_native
    , coalesce(daily_supply_data.premine_unlocks_native, 0) as premine_unlocks_native
    , coalesce(daily_supply_data.burns_native, 0) as burns_native
    , coalesce(daily_supply_data.emissions_native, 0) + coalesce(daily_supply_data.premine_unlocks_native, 0) - coalesce(daily_supply_data.burns_native, 0) as net_supply_change_native
    , sum(net_supply_change_native) over (order by date rows between unbounded preceding and current row) as circulating_supply_native
from date_spine
left join market_metrics using (date)
left join bridge_metrics using (date)
left join bridge_volume using (date)
left join daily_supply_data using (date)
where date_spine.date < to_date(sysdate())
order by 1