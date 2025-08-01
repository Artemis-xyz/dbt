{{
    config(
        materialized="incremental",
        snowflake_warehouse="LIVEPEER",
        database="livepeer",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

with market_data as (
    {{ get_coingecko_metrics("livepeer") }}
)
, revenue as (
    SELECT
        date,
        fees
    FROM {{ ref("fact_livepeer_revenue") }}
)
SELECT
    market_data.date,
    revenue.fees,
    market_data.price,
    market_data.market_cap,
    market_data.fdmc,
    market_data.token_turnover_circulating,
    market_data.token_turnover_fdv,
    market_data.token_volume,
    -- timestamp columns
    sysdate() as created_on,
    sysdate() as modified_on
FROM market_data
LEFT JOIN revenue USING(date)
where true
{{ ez_metrics_incremental('market_data.date', backfill_date) }}
and market_data.date < to_date(sysdate())