{{
    config(
        materialized="incremental",
        snowflake_warehouse="HAEDAL",
        database="haedal",
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

WITH 
    defillama_tvl AS (
        SELECT
            date, 
            SUM(tvl) AS tvl
        FROM {{ ref("fact_defillama_protocol_tvls") }}
        WHERE defillama_protocol_id = 3489 OR defillama_protocol_id = 5967 OR defillama_protocol_id = 5784
            -- This includes Haedal Protocol (Liquid Staking), Haedal AMM, and Haedal Vault (Farming)
        GROUP BY 1
    )

    , date_spine AS (
        SELECT date
        FROM {{ ref("dim_date_spine") }}
        WHERE date >= (SELECT MIN(date) FROM defillama_tvl)
          AND date < to_date(sysdate())
    )

    , defillama_tvl_forwardfill AS (
        SELECT
            d.date,
            LAST_VALUE(t.tvl IGNORE NULLS) OVER (
                ORDER BY d.date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS tvl
        FROM date_spine d
        LEFT JOIN defillama_tvl t ON d.date = t.date
    )

    , market_data AS (
        {{ get_coingecko_metrics("haedal") }}
    )

SELECT
    d.date, 
    'haedal' AS app,
    'DeFi' AS category,
    COALESCE(m.price, 0) AS price,
    COALESCE(m.market_cap, 0) AS market_cap,
    COALESCE(m.fdmc, 0) AS fdmc,
    COALESCE(m.token_volume, 0) AS token_volume,
    COALESCE(m.token_turnover_circulating, 0) AS token_turnover_circulating,
    COALESCE(m.token_turnover_fdv, 0) AS token_turnover_fdv,
    COALESCE(d.tvl, 0) AS tvl,
    -- timestamp columns
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on,
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM defillama_tvl_forwardfill d
LEFT JOIN market_data m USING (date)
where true
{{ ez_metrics_incremental('d.date', backfill_date) }}
and d.date < to_date(sysdate())