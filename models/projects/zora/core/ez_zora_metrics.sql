{{
    config(
        materialized="incremental"
        , snowflake_warehouse="ZORA"
        , database="zora"
        , schema="core"
        , alias="ez_metrics"
        , incremental_strategy="merge"
        , unique_key="date"
        , on_schema_change="append_new_columns"
        , merge_update_columns=var("backfill_columns", [])
        , merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none
        , full_refresh=false
        , tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

WITH
    date_spine AS (
        SELECT date
        FROM {{ ref("dim_date_spine") }}
        WHERE date >= '2023-06-13'
        AND date < to_date(sysdate())
    )
    , fundamental_data AS (
        SELECT
            date
            , txns
            , daa AS dau
            , gas_usd AS fees
            , gas AS fees_native
            , median_gas AS median_txn_fee
            , revenue
            , revenue_native
            , l1_data_cost
            , l1_data_cost_native
        FROM {{ ref("fact_zora_txns") }}
        LEFT JOIN {{ ref("fact_zora_daa") }} USING (date)
        LEFT JOIN {{ ref("fact_zora_gas_gas_usd_revenue") }} USING (date)
    )
    , github_data AS ({{ get_github_metrics("zora") }})
    , contract_data AS ({{ get_contract_metrics("zora") }})
    , defillama_data AS ({{ get_defillama_metrics("zora") }})
    , rolling_metrics AS ({{ get_rolling_active_address_metrics("zora") }})
    , zora_dex_volumes as (
        SELECT
            date
            , daily_volume AS dex_volumes
            , daily_volume_adjusted AS adjusted_dex_volumes
        FROM {{ ref("fact_zora_daily_dex_volumes") }}
    )
    , market_data AS ({{get_coinmarketcap_metrics("zora")}})
select
    date_spine.date
    , 'zora' AS artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Data
    , dau AS chain_dau
    , dau 
    , wau AS chain_wau
    , mau AS chain_mau
    , txns AS chain_txns
    , txns
    , tvl
    , tvl AS chain_tvl
    , median_txn_fee AS chain_median_txn_fee
    , avg_txn_fee AS chain_avg_txn_fee
    , dune_dex_volumes_zora.dex_volumes AS chain_spot_volume
    , dune_dex_volumes_zora.adjusted_dex_volumes AS chain_spot_volume_adjusted

    -- Cash Flow Metrics
    , fees_native
    , fees
    , l1_data_cost AS l1_fee_allocation
    , revenue AS treasury_fee_allocation

    -- Financial Statements
    , revenue
    , revenue AS earnings

    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , weekly_contracts_deployed
    , weekly_contract_deployers

    -- Turnover Data
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
FROM date_spine
LEFT JOIN fundamental_data USING (date)
LEFT JOIN github_data USING (date)
LEFT JOIN contract_data USING (date)
LEFT JOIN defillama_data USING (date)
LEFT JOIN rolling_metrics USING (date)
LEFT JOIN zora_dex_volumes AS dune_dex_volumes_zora ON fundamental_data.date = dune_dex_volumes_zora.date
WHERE true
{{ ez_metrics_incremental("fundamental_data.date", backfill_date) }}
AND fundamental_data.date < to_date(sysdate())