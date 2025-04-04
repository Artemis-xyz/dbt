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
        , sum(bridge_daa) as bridge_daa
        , sum(fees) as fees
        , sum(bridge_txns) as bridge_txns
    FROM {{ ref('ez_layerzero_metrics_by_chain') }}
    GROUP BY 1
)
, market_data as (
    {{ get_coingecko_metrics("layerzero") }}
)

SELECT
    date
    , coalesce(bridge_daa, 0) as bridge_daa
    , coalesce(fees, 0) as fees

    -- Standardized Metrics

    -- Token Metrics
    , coalesce(market_data.price, 0) as price
    , coalesce(market_data.market_cap, 0) as market_cap
    , coalesce(market_data.fdmc, 0) as fdmc
    , coalesce(market_data.token_volume, 0) as token_volume

    -- Bridge Metrics
    , coalesce(bridge_daa, 0) as bridge_dau
    , coalesce(fees, 0) as bridge_fees
    , coalesce(bridge_txns, 0) as bridge_txns
    , coalesce(bridge_volume, 0) as bridge_volume

    -- Turnover Metrics
    , coalesce(market_data.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(market_data.token_turnover_fdv, 0) as token_turnover_fdv
FROM {{ ref('ez_layerzero_metrics_by_chain') }}
LEFT JOIN bridge_volume using (date)
LEFT JOIN market_data using (date)
WHERE date < to_date(sysdate())
ORDER BY 1