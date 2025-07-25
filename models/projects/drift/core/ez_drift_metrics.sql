{{
    config(
        materialized="incremental",
        snowflake_warehouse="DRIFT",
        database="drift",
        schema="core",
        alias="ez_metrics",
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

WITH parsed_log_metrics AS (
    SELECT 
        block_date AS date,
        SUM(IFF(market_type = 1, total_taker_fee, 0)) AS perp_fees,
        SUM(IFF(market_type = 1, total_revenue, 0)) AS perp_revenue,
        SUM(IFF(market_type = 1, total_volume, 0)) AS perp_trading_volume,
        SUM(IFF(market_type = 0, IFF(total_revenue < 0, 0, total_revenue), 0)) AS spot_fees,
        SUM(IFF(market_type = 0, total_taker_fee, 0)) AS spot_revenue,
        SUM(IFF(market_type = 0, total_volume, 0)) AS spot_trading_volume
    FROM {{ ref("fact_drift_parsed_logs") }}
    GROUP BY
        block_date
)
, price_data as ({{ get_coingecko_metrics("drift-protocol") }})
, defillama_data as ({{ get_defillama_protocol_metrics("drift trade") }})
, supply_data as ( select * from {{ ref("fact_drift_supply_data") }})
, open_interest as ( select * from {{ref("fact_drift_open_interest")}})
SELECT 
    coalesce(
        price_data.date,
        fact_drift_float_borrow_lending_revenue.date,
        defillama_data.date,
        parsed_log_metrics.date,
        fact_drift_amm_revenue.date
    ) as date,
    'drift' AS app,
    'DeFi' AS category,
    daily_avg_float_revenue as float_revenue,
    daily_avg_lending_revenue as lending_revenue,
    parsed_log_metrics.perp_revenue,
    parsed_log_metrics.perp_trading_volume as trading_volume,
    parsed_log_metrics.spot_revenue,
    parsed_log_metrics.spot_trading_volume,
    total_revenue as excess_pnl_daily_change,
    coalesce(float_revenue, 0) + 
    coalesce(lending_revenue, 0) +
    coalesce(parsed_log_metrics.perp_revenue, 0) +
    coalesce(parsed_log_metrics.spot_revenue, 0) as old_revenue,
    coalesce(float_revenue, 0) + 
    coalesce(lending_revenue, 0) +
    coalesce(parsed_log_metrics.perp_revenue, 0) +
    coalesce(parsed_log_metrics.spot_revenue, 0) as revenue,
    total_revenue - (coalesce(float_revenue, 0) + 
    coalesce(lending_revenue, 0) +
    coalesce(parsed_log_metrics.perp_revenue, 0) +
    coalesce(parsed_log_metrics.spot_revenue, 0)) as amm_revenue,
    coalesce(parsed_log_metrics.perp_fees + parsed_log_metrics.spot_fees, 0) as fees,
    latest_excess_pnl as daily_latest_excess_pnl
    -- Standardized Metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume
    -- Usage Metrics
    , parsed_log_metrics.perp_trading_volume as perp_volume
    , parsed_log_metrics.spot_trading_volume as spot_volume
    , defillama_data.tvl
    -- Cashflow Metrics
    , parsed_log_metrics.perp_fees as perp_fees
    , parsed_log_metrics.spot_fees as spot_fees
    , coalesce(parsed_log_metrics.perp_fees + parsed_log_metrics.spot_fees, 0) as ecosystem_revenue
    -- TODO: Add cashflows to individual entities
    -- Supply Metrics
    , supply_data.premine_unlocks
    , supply_data.gross_emissions
    , supply_data.net_supply_change
    , supply_data.circulating_supply as circulating_supply_native

    -- Other Metrics
    , token_turnover_circulating
    , token_turnover_fdv
    , open_interest
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM price_data 
LEFT JOIN {{ ref("fact_drift_amm_revenue") }} as fact_drift_amm_revenue
    ON price_data.date = fact_drift_amm_revenue.date
FULL JOIN {{ ref("fact_drift_float_borrow_lending_revenue") }} as fact_drift_float_borrow_lending_revenue
    ON price_data.date = fact_drift_float_borrow_lending_revenue.date
FULL JOIN defillama_data
    ON price_data.date = defillama_data.date
FULL JOIN parsed_log_metrics
    ON price_data.date = parsed_log_metrics.date
LEFT JOIN supply_data
    ON price_data.date = supply_data.date
LEFT JOIN open_interest
    ON price_data.date = open_interest.date
where true
and coalesce(
    price_data.date,
    fact_drift_float_borrow_lending_revenue.date,
    defillama_data.date,
    parsed_log_metrics.date,
    fact_drift_amm_revenue.date
) is not null
{{ ez_metrics_incremental('coalesce(
    price_data.date,
    fact_drift_float_borrow_lending_revenue.date,
    defillama_data.date,
    parsed_log_metrics.date,
    fact_drift_amm_revenue.date
)', backfill_date) }}
and coalesce(
    price_data.date,
    fact_drift_float_borrow_lending_revenue.date,
    defillama_data.date,
    parsed_log_metrics.date,
    fact_drift_amm_revenue.date
) < to_date(sysdate())