-- depends_on {{ ref("fact_base_transactions_v2") }}
{{
    config(
        materialized="incremental",
        snowflake_warehouse="BASE",
        database="base",
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

{% set backfill_date = var("backfill_date", None) %}

with
    date_spine AS (
        SELECT date
        FROM {{ ref("dim_date_spine") }}
        WHERE date >= '2023-06-15' AND date < TO_DATE(SYSDATE())
    )
    , fundamental_data AS ({{ get_fundamental_data_for_chain("base", "v2") }})
    , defillama_data AS ({{ get_defillama_metrics("base") }})
    , stablecoin_data AS ({{ get_stablecoin_metrics("base") }})
    , contract_data AS ({{ get_contract_metrics("base") }})
    , expenses_data AS (
        SELECT date, chain, l1_data_cost_native, l1_data_cost
        FROM {{ ref("fact_base_l1_data_cost") }}
    )  -- supply side revenue and fees
    , nft_metrics AS ({{ get_nft_metrics("base") }})
    , p2p_metrics AS ({{ get_p2p_metrics("base") }})
    , rolling_metrics AS ({{ get_rolling_active_address_metrics("base") }})
    , bridge_volume_metrics AS (
        SELECT date, bridge_volume
        FROM {{ ref("fact_base_bridge_bridge_volume") }}
        WHERE chain IS NULL
    )
    , bridge_daa_metrics AS (
        SELECT date, bridge_daa
        FROM {{ ref("fact_base_bridge_bridge_daa") }}
    )
    , base_dex_volumes AS (
        SELECT date, daily_volume AS dex_volumes, daily_volume_adjusted AS adjusted_dex_volumes
        FROM {{ ref("fact_base_daily_dex_volumes") }}
    )
    , adjusted_dau_metrics AS (
        SELECT date, adj_daus AS adjusted_dau
        FROM {{ ref("ez_base_adjusted_dau") }}
    )

SELECT
    date_spine.date
    , 'base' AS artemis_id
    
    -- Standardized Metrics

    -- Usage Data
    , fundamental_data.dau AS chain_dau
    , adjusted_dau_metrics.adjusted_dau AS chain_dau_adjusted
    , fundamental_data.wau AS chain_wau
    , fundamental_data.mau AS chain_mau
    , fundamental_data.dau
    , fundamental_data.txns AS chain_txns
    , fundamental_data.txns
    , base_dex_volumes.dex_volumes AS chain_spot_volume
    , base_dex_volumes.adjusted_dex_volumes AS chain_spot_volume_adjusted
    , nft_metrics.nft_trading_volume AS chain_nft_trading_volume
    , fundamental_data.tvl AS chain_tvl
    , fundamental_data.tvl
    , fundamental_data.avg_txn_fee AS chain_avg_txn_fee
    , fundamental_data.median_txn_fee AS chain_median_txn_fee
    , fundamental_data.dau_over_100 AS chain_dau_over_100_balance
    , fundamental_data.sybil_users
    , fundamental_data.non_sybil_users
    , fundamental_data.returning_users
    , fundamental_data.new_users
    , fundamental_data.low_sleep_users
    , fundamental_data.high_sleep_users
    , p2p_metrics.p2p_native_transfer_volume
    , p2p_metrics.p2p_token_transfer_volume
    , p2p_metrics.p2p_transfer_volume
    , COALESCE(artemis_stablecoin_transfer_volume, 0) - COALESCE(stablecoin_data.p2p_stablecoin_transfer_volume, 0) AS non_p2p_stablecoin_transfer_volume
    , COALESCE(base_dex_volumes.dex_volumes, 0) + COALESCE(nft_metrics.nft_trading_volume, 0) + COALESCE(p2p_metrics.p2p_transfer_volume, 0) AS settlement_volume

    -- Fee Data
    , fundamental_data.fees_native
    , fundamental_data.fees
    , expenses_data.l1_data_cost AS l1_fee_allocation
    , COALESCE(fundamental_data.fees_native, 0) - COALESCE(expenses_data.l1_data_cost_native, 0) AS treasury_fee_allocation_native
    , COALESCE(fundamental_data.fees, 0) - COALESCE(expenses_data.l1_data_cost, 0) AS treasury_fee_allocation

    -- Financial Statements
    , COALESCE(fundamental_data.fees, 0) - COALESCE(expenses_data.l1_data_cost, 0) AS revenue
    , revenue AS earnings

    -- Developer Metrics
    , contract_data.weekly_contracts_deployed
    , contract_data.weekly_contract_deployers

    -- Stablecoin Metrics
    , stablecoin_data.stablecoin_total_supply
    , stablecoin_data.stablecoin_txns
    , stablecoin_data.stablecoin_dau
    , stablecoin_data.stablecoin_mau
    , stablecoin_data.stablecoin_transfer_volume
    , stablecoin_data.stablecoin_tokenholder_count
    , stablecoin_data.artemis_stablecoin_txns
    , stablecoin_data.artemis_stablecoin_dau
    , stablecoin_data.artemis_stablecoin_mau
    , stablecoin_data.artemis_stablecoin_transfer_volume
    , stablecoin_data.p2p_stablecoin_tokenholder_count
    , stablecoin_data.p2p_stablecoin_txns
    , stablecoin_data.p2p_stablecoin_dau
    , stablecoin_data.p2p_stablecoin_mau
    , stablecoin_data.p2p_stablecoin_transfer_volume

    -- Bridge Metrics
    , bridge_volume_metrics.bridge_volume
    , bridge_daa_metrics.bridge_daa

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
FROM date_spine
LEFT JOIN fundamental_data USING (date)
LEFT JOIN defillama_data USING (date)
LEFT JOIN stablecoin_data USING (date)
LEFT JOIN expenses_data USING (date)
LEFT JOIN contract_data USING (date)
LEFT JOIN nft_metrics USING (date)
LEFT JOIN p2p_metrics USING (date)
LEFT JOIN rolling_metrics USING (date)
LEFT JOIN bridge_volume_metrics USING (date)
LEFT JOIN bridge_daa_metrics USING (date)
LEFT JOIN base_dex_volumes USING (date)
LEFT JOIN adjusted_dau_metrics USING (date)
WHERE true
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
AND fundamental_data.date < TO_DATE(SYSDATE())
