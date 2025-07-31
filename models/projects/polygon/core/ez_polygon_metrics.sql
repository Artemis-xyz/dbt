-- depends_on {{ ref("fact_polygon_transactions_v2") }}
{{
    config(
        materialized="incremental",
        snowflake_warehouse="polygon",
        database="polygon",
        schema="core",
        alias="ez_metrics",
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

with
    date_spine AS (
        SELECT date
        FROM {{ ref("dim_date_spine")}}
        WHERE date >= '2020-05-30' AND date < to_date(sysdate())
    )
    , fundamental_data AS ({{ get_fundamental_data_for_chain("polygon", "v2") }})
    , market_data AS ({{ get_coingecko_metrics("matic-network") }})
    , defillama_data AS ({{ get_defillama_metrics("polygon") }})
    , stablecoin_data AS ({{ get_stablecoin_metrics("polygon") }})
    , github_data AS ({{ get_github_metrics("polygon") }})
    , contract_data AS ({{ get_contract_metrics("polygon") }})
    , revenue_data AS (
        SELECT 
            date
            , native_token_burn AS revenue_native
            , revenue
        FROM {{ ref("agg_daily_polygon_revenue") }}
    )
    , l1_cost_data AS (
        SELECT 
            date
            , SUM(tx_fee) AS l1_data_cost_native
            , SUM(gas_usd) AS l1_data_cost
        FROM {{ ref("fact_ethereum_transactions_v2") }}
        WHERE LOWER(contract_address) = LOWER('0x86E4Dc95c7FBdBf52e33D563BbDB00823894C287')   
        GROUP BY date
    )
    , nft_metrics AS ({{ get_nft_metrics("polygon") }})
    , p2p_metrics AS ({{ get_p2p_metrics("polygon") }})
    , rolling_metrics AS ({{ get_rolling_active_address_metrics("polygon") }})
    , bridge_volume_metrics AS (
        SELECT 
            date
            , bridge_volume
        FROM {{ ref("fact_polygon_pos_bridge_bridge_volume") }}
        WHERE chain IS NULL
    )
    , bridge_daa_metrics AS (
        SELECT 
            date
            , bridge_daa
        FROM {{ ref("fact_polygon_pos_bridge_bridge_daa") }}
    )
    , polygon_dex_volumes AS (
        SELECT 
            date
            , daily_volume AS dex_volumes
            , daily_volume_adjusted AS adjusted_dex_volumes
        FROM {{ ref("fact_polygon_daily_dex_volumes") }}
    )
    , token_incentives AS (
        SELECT 
            date
            , token_incentives
        FROM {{ ref("fact_polygon_token_incentives") }}
    )

SELECT
    date_spine.date
    , 'polygon' AS artemis_id

    -- Standardized Metrics

    -- Market Data 
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Data
    , fundamental_data.dau AS chain_dau
    , fundamental_data.dau
    , fundamental_data.wau AS chain_wau
    , fundamental_data.mau AS chain_mau
    , fundamental_data.txns AS chain_txns
    , fundamental_data.txns 
    , fundamental_data.tvl AS chain_tvl
    , fundamental_data.tvl
    , fundamental_data.avg_txn_fee AS chain_avg_txn_fee
    , fundamental_data.median_txn_fee AS chain_median_txn_fee
    , fundamental_data.returning_users
    , fundamental_data.new_users
    , fundamental_data.sybil_users
    , fundamental_data.non_sybil_users
    , fundamental_data.low_sleep_users
    , fundamental_data.high_sleep_users
    , fundamental_data.dau_over_100 AS dau_over_100_balance
    , nft_metrics.nft_trading_volume AS chain_nft_trading_volume
    , p2p_metrics.p2p_native_transfer_volume
    , p2p_metrics.p2p_token_transfer_volume
    , p2p_metrics.p2p_transfer_volume
    , coalesce(stablecoin_data.artemis_stablecoin_transfer_volume, 0) - coalesce(stablecoin_data.p2p_stablecoin_transfer_volume, 0) as non_p2p_stablecoin_transfer_volume
    , coalesce(polygon_dex_volumes.dex_volumes, 0) + coalesce(nft_metrics.nft_trading_volume, 0) + coalesce(p2p_metrics.p2p_transfer_volume, 0) as settlement_volume
    , polygon_dex_volumes.dex_volumes AS chain_spot_volume
    , polygon_dex_volumes.adjusted_dex_volumes AS chain_spot_volume_adjusted

    -- Fee Data
    , fundamental_data.fees_native
    , fundamental_data.fees AS chain_fees
    , fundamental_data.fees 
    , revenue_data.revenue AS validator_fee_allocation
    , l1_cost_data.l1_data_cost AS l1_fee_allocation

    -- Financial Statements
    , revenue_data.revenue_native
    , revenue_data.revenue
    , coalesce(token_incentives.token_incentives, 0) as token_incentives
    , revenue_data.revenue - token_incentives.token_incentives AS earnings

    -- Developer Metrics
    , github_data.weekly_commits_core_ecosystem
    , github_data.weekly_commits_sub_ecosystem
    , github_data.weekly_developers_core_ecosystem
    , github_data.weekly_developers_sub_ecosystem
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

    -- Turnover Data
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv

    -- Timestamp Columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
FROM date_spine
LEFT JOIN fundamental_data USING (date)
LEFT JOIN market_data USING (date)
LEFT JOIN defillama_data USING (date)
LEFT JOIN stablecoin_data USING (date)
LEFT JOIN github_data USING (date)
LEFT JOIN contract_data USING (date)
LEFT JOIN revenue_data USING (date)
LEFT JOIN nft_metrics USING (date)
LEFT JOIN p2p_metrics USING (date)
LEFT JOIN rolling_metrics USING (date)
LEFT JOIN l1_cost_data USING (date)
LEFT JOIN bridge_volume_metrics USING (date)
LEFT JOIN bridge_daa_metrics USING (date)
LEFT JOIN polygon_dex_volumes USING (date)
LEFT JOIN token_incentives ti USING (date)
WHERE true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
AND date_spine.date < to_date(sysdate())
