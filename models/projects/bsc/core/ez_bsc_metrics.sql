-- depends_on {{ ref("fact_bsc_transactions_v2") }}
{{
    config(
        materialized="incremental",
        snowflake_warehouse="BSC_SM",
        database="bsc",
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

WITH
    date_spine AS (
        SELECT date
        FROM {{ ref("dim_date_spine") }}
        WHERE date >= '2020-08-29' AND date < TO_DATE(SYSDATE())
    )
    , fundamental_data AS ({{ get_fundamental_data_for_chain("bsc", "v2") }})
    , market_data AS ({{ get_coingecko_metrics("binancecoin") }})
    , defillama_data AS ({{ get_defillama_metrics("bsc") }})
    , stablecoin_data AS ({{ get_stablecoin_metrics("bsc") }})
    , github_data AS ({{ get_github_metrics("Binance Smart Chain") }})
    , contract_data AS ({{ get_contract_metrics("bsc") }})
    , nft_metrics AS ({{ get_nft_metrics("bsc") }})
    , rolling_metrics AS ({{ get_rolling_active_address_metrics("bsc") }})
    , binance_dex_volumes AS (
        SELECT date, daily_volume AS dex_volumes, daily_volume_adjusted AS adjusted_dex_volumes
        FROM {{ ref("fact_binance_daily_dex_volumes") }}
    )
    , staked_eth_metrics AS (
        SELECT
            date
            , SUM(num_staked_eth) AS num_staked_eth
            , SUM(amount_staked_usd) AS amount_staked_usd
            , SUM(num_staked_eth_net_change) AS num_staked_eth_net_change
            , SUM(amount_staked_usd_net_change) AS amount_staked_usd_net_change
        FROM {{ ref('fact_binance_staked_eth_count_with_usd_and_change') }}
        GROUP BY 1
    )
SELECT
    date_spine.date
    'bsc' AS artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Data
    , dau AS chain_dau
    , dau 
    , wau AS chain_mau
    , mau AS chain_mau
    , txns AS chain_txns
    , txns
    , tvl AS chain_tvl
    , tvl 
    , nft_trading_volume AS chain_nft_trading_volume
    , dune_dex_volumes_binance.dex_volumes AS chain_spot_volume
    , dune_dex_volumes_binance.adjusted_dex_volumes AS chain_spot_volume_adjusted
    , staked_eth_metrics.num_staked_eth as lst_tvl_native
    , staked_eth_metrics.amount_staked_usd as lst_tvl
    , staked_eth_metrics.num_staked_eth_net_change as lst_tvl_native_net_change
    , staked_eth_metrics.amount_staked_usd_net_change as lst_tvl_net_change

    -- Fee Data
    , fees_native
    , fees
    , fees as chain_fees
    , fees * .1 AS burned_fee_allocation

    -- Financial Statements
    , fees * .1 AS revenue
    , revenue AS earnings

    -- Developer metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , weekly_contracts_deployed
    , weekly_contract_deployers

    -- Stablecoin metrics
    , stablecoin_total_supply
    , stablecoin_txns
    , stablecoin_dau
    , stablecoin_mau
    , stablecoin_transfer_volume
    , stablecoin_tokenholder_count
    , artemis_stablecoin_txns
    , artemis_stablecoin_dau
    , artemis_stablecoin_mau
    , artemis_stablecoin_transfer_volume
    , p2p_stablecoin_tokenholder_count
    , p2p_stablecoin_txns
    , p2p_stablecoin_dau
    , p2p_stablecoin_mau
    , p2p_stablecoin_transfer_volume

    -- Turnover Data
    , token_turnover_circulating
    , token_turnover_fdv

    -- Bespoke Metrics
    , returning_users
    , new_users
    , low_sleep_users
    , high_sleep_users
    , sybil_users
    , non_sybil_users
    , dau_over_100 AS dau_over_100_balance

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
FROM date_spine
LEFT JOIN fundamental_data USING (date)
LEFT JOIN market_data USING (date)
LEFT JOIN defillama_data USING (date)
LEFT JOIN stablecoin_data USING (date)
LEFT JOIN github_data USING (date)
LEFT JOIN contract_data USING (date)
LEFT JOIN nft_metrics USING (date)
LEFT JOIN rolling_metrics USING (date)
LEFT JOIN binance_dex_volumes AS dune_dex_volumes_binance USING (date)
LEFT JOIN staked_eth_metrics USING (date)
WHERE true
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
    AND fundamental_data.date < TO_DATE(SYSDATE())
