{{
    config(
        materialized="incremental",
        snowflake_warehouse="POLYMARKET",
        database="polymarket",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

SELECT 
    date,
    'polymarket' AS app,
    'DeFi' AS category,
    trump_prediction_market_100k_buy_order_price,
    kamala_prediction_market_100k_buy_order_price,
    trump_prediction_market_100k_sell_order_price,
    kamala_prediction_market_100k_sell_order_price,
    -- timestamp columns
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on,    
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM {{ ref("fact_polymarket_prediction_markets") }}
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())