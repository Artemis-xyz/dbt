{{
    config(
        materialized="incremental",
        snowflake_warehouse="POLYGON_ZK",
        database="polygon_zk",
        schema="core",
        alias="ez_metrics",
        enabled=false,
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
    date_spine as (
        SELECT date
        FROM {{ ref("dim_date_spine") }}
        WHERE date >= '2023-03-24'
        AND date < to_date(sysdate())
    )
    , fundamental_data AS (
        SELECT 
            date
            , chain
            , daa AS dau
            , txns
            , gas AS fees_native
            , gas_usd AS fees
        FROM {{ ref("fact_polygon_zk_daa_txns_gas_usd") }}
    )
    , market_data AS ({{ get_coingecko_metrics("polygon-ecosystem-token") }})
    , defillama_data AS ({{ get_defillama_metrics("polygon zkevm") }})
    , l1_data_cost AS (
        SELECT 
            date
            , l1_data_cost_native
            , l1_data_cost
        FROM {{ ref("fact_polygon_zk_l1_data_cost") }}
    )
    , github_data AS ({{ get_github_metrics("Polygon Hermez") }})
    , polygon_zk_dex_volumes AS (
        SELECT 
            date
            , daily_volume AS dex_volumes
        FROM {{ ref("fact_polygon_zk_daily_dex_volumes") }}
    )
SELECT
    date_spine.date
    , 'polygon_zk' AS artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Data
    , dau AS chain_dau
    , dau 
    , txns AS chain_txns
    , txns
    , tvl AS chain_tvl
    , tvl
    , fees / txns AS chain_avg_txn_fee
    , dune_dex_volumes_polygon_zk.dex_volumes AS chain_spot_volume

    -- Fee Data
    , fees AS chain_fees
    , fees
    , COALESCE(fees, 0) - l1_data_cost AS service_fee_allocation
    , l1_data_cost AS l1_fee_allocation

    -- Financial Statements
    , COALESCE(fees, 0) - l1_data_cost AS revenue
    , revenue AS earnings 

    -- Developer Data
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem

    -- Turnover Data
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
FROM date_spine
LEFT JOIN fundamental_data USING (date)
LEFT JOIN market_data USING (date)
LEFT JOIN defillama_data USING (date)
LEFT JOIN l1_data_cost USING (date)
LEFT JOIN github_data USING (date)
LEFT JOIN polygon_zk_dex_volumes AS dune_dex_volumes_polygon_zk USING (date)
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
AND date_spine.date < to_date(sysdate())
GROUP BY ALL
