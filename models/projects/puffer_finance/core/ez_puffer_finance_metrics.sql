{{
    config(
        materialized="incremental",
        snowflake_warehouse="PUFFER_FINANCE",
        databASe="puffer_finance",
        schema="core",
        aliAS="ez_metrics",
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

WITH 
    restaked_eth_metrics AS (
        SELECT
            date,
            chain,
            num_restaked_eth,
            amount_restaked_usd,
            num_restaked_eth_net_change,
            amount_restaked_usd_net_change
        FROM {{ ref('fact_puffer_finance_restaked_eth_count_with_usd_and_change') }}
    )
    , date_spine AS (
        SELECT
            ds.date
        FROM {{ ref('dim_date_spine') }} ds
        WHERE ds.date BETWEEN (SELECT min(date) FROM restaked_eth_metrics) AND to_date(sysdate())
    )
    , market_data AS (
        {{get_coingecko_metrics('puffer-finance')}}
    )

SELECT
    date_spine.date,
    'puffer_finance' AS artemis_id,

    --Standardized Metrics

    --Market Data
    , market_data.price AS price
    , market_data.token_volume AS token_volume
    , market_data.market_cap AS market_cap
    , market_data.fdmc AS fdmc

    --Usage Data
    , restaked_eth_metrics.amount_restaked_usd AS lrt_tvl
    , restaked_eth_metrics.amount_restaked_usd AS tvl

    --Turnover Data
    , market_data.token_turnover_circulating AS token_turnover_circulating
    , market_data.token_turnover_fdv AS token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
FROM date_spine
LEFT JOIN restaked_eth_metrics USING (date)
LEFT JOIN market_data USING (date)
WHERE true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
AND date_spine.date < to_date(sysdate())
