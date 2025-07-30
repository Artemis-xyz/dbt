-- depends_on {{ ref("fact_ethereum_transactions_v2") }}
-- depends_on {{ ref('fact_ethereum_block_producers_silver') }}
-- depends_on {{ ref('fact_ethereum_amount_staked_silver') }}
-- depends_on {{ ref('fact_ethereum_p2p_transfer_volume') }}

{{
    config(
        materialized="incremental",
        snowflake_warehouse="ETHEREUM",
        database="ethereum",
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
        WHERE date >= '2015-08-07'
        AND date < to_date(sysdate())
    )
    , fundamental_data AS ({{ get_fundamental_data_for_chain("ethereum", "v2") }})
    , market_data AS ({{ get_coingecko_metrics("ethereum") }})
    , defillama_data AS ({{ get_defillama_metrics("ethereum") }})
    , stablecoin_data AS ({{ get_stablecoin_metrics("ethereum") }})
    , staking_data AS ({{ get_staking_metrics("ethereum") }})
    , censored_block_metrics AS ({{ get_censored_block_metrics("ethereum") }})
    , revenue_data AS (
        SELECT 
            date
            , revenue
            , native_token_burn AS revenue_native
        FROM {{ ref("agg_daily_ethereum_revenue") }}
    )
    , github_data AS ({{ get_github_metrics("ethereum") }})
    , contract_data AS ({{ get_contract_metrics("ethereum") }})
    , validator_queue_data AS (
        SELECT 
            date
            , queue_entry_amount
            , queue_exit_amount
            , queue_active_amount
        FROM {{ ref("fact_ethereum_beacon_chain_queue_entry_active_exit_silver") }}
    )
    , nft_metrics AS ({{ get_nft_metrics("ethereum") }})
    , p2p_metrics AS ({{ get_p2p_metrics("ethereum") }})
    , rolling_metrics AS ({{ get_rolling_active_address_metrics("ethereum") }})
    , da_metrics AS (
        SELECT 
            date
            , blob_fees_native
            , blob_fees
            , blob_size_mib
            , avg_mib_per_second
            , avg_cost_per_mib
            , avg_cost_per_mib_gwei
            , submitters
        FROM {{ ref("fact_ethereum_da_metrics") }}
    )
    , etf_metrics AS (
        SELECT
            date
            , SUM(net_etf_flow_native) AS net_etf_flow_native
            , SUM(net_etf_flow) AS net_etf_flow
            , SUM(cumulative_etf_flow_native) AS cumulative_etf_flow_native
            , SUM(cumulative_etf_flow) AS cumulative_etf_flow
        FROM {{ ref("ez_ethereum_etf_metrics") }}
        GROUP BY 1
    )
    , ethereum_dex_volumes AS (
        SELECT 
            date
            , SUM(daily_volume) AS dex_volumes
            , SUM(daily_volume_adjusted) AS adjusted_dex_volumes
        FROM {{ ref("fact_ethereum_daily_dex_volumes") }}
        GROUP BY 1
    )
    , block_rewards_data AS (
        SELECT 
            date
            , block_rewards_native
        FROM {{ ref("fact_ethereum_block_rewards") }}
    )
    , adjusted_dau_metrics AS (
        SELECT 
            date
            , adj_daus AS adjusted_dau
        FROM {{ ref("ez_ethereum_adjusted_dau") }}
    )
    , eth_supply AS (
        SELECT 
            date
            , issued_supply
            , circulating_supply
        FROM {{ ref("fact_ethereum_eth_supply_estimated") }}
    )
    , application_fees AS (
        SELECT 
            DATE_TRUNC(DAY, date) AS date 
            , SUM(COALESCE(fees, 0)) AS application_fees
        FROM {{ ref("ez_protocol_datahub_by_chain") }}
        WHERE chain = 'ethereum'
        GROUP BY 1
    )

select
    date_spine.date
    , 'ethereum' as artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Data
    , fundamental_data.dau AS chain_dau
    , adjusted_dau.adjusted_dau AS chain_dau_adjusted
    , fundamental_data.dau
    , fundamental_data.txns AS chain_txns
    , fundamental_data.txns
    , wau AS chain_wau
    , mau AS chain_mau
    , tvl AS chain_tvl
    , tvl 
    , avg_txn_fee AS chain_avg_txn_fee
    , median_txn_fee AS chain_median_txn_fee
    , returning_users
    , new_users
    , low_sleep_users
    , high_sleep_users
    , sybil_users
    , non_sybil_users
    , dau_over_100 AS dau_over_100_balance
    , censored_blocks
    , semi_censored_blocks
    , non_censored_blocks
    , total_blocks_produced
    , percent_censored AS percent_censored_blocks
    , percent_semi_censored AS percent_semi_censored_blocks
    , percent_non_censored AS percent_non_censored_blocks
    , total_staked_native
    , total_staked_usd
    , total_staked_usd AS total_staked
    , queue_entry_amount
    , queue_exit_amount
    , queue_active_amount
    , nft_trading_volume AS chain_nft_trading_volume
    , p2p_native_transfer_volume
    , p2p_token_transfer_volume
    , p2p_transfer_volume
    , COALESCE(artemis_stablecoin_transfer_volume, 0) - COALESCE(stablecoin_data.p2p_stablecoin_transfer_volume, 0) AS non_p2p_stablecoin_transfer_volume
    , COALESCE(ethereum_dex_volumes.dex_volumes, 0) + COALESCE(nft_trading_volume, 0) + COALESCE(p2p_transfer_volume, 0) AS settlement_volume
    , blob_fees_native
    , blob_fees
    , blob_size_mib
    , avg_mib_per_second
    , avg_cost_per_mib_gwei
    , avg_cost_per_mib
    , submitters AS da_dau
    , ethereum_dex_volumes.dex_volumes AS chain_spot_volume
    , COALESCE(fees, 0) + COALESCE(blob_fees, 0) + COALESCE(priority_fee_usd, 0) + COALESCE(settlement_volume, 0) + COALESCE(application_fees.application_fees, 0) AS total_economic_activity
    , ethereum_dex_volumes.dex_volumes AS chain_spot_volume
    , ethereum_dex_volumes.adjusted_dex_volumes AS chain_spot_volume_adjusted

    -- Fee Data
    , fees_native
    , priority_fee_usd AS priority_fees
    , fees AS chain_fees
    , blob_fees 
    , COALESCE(fees, 0) + COALESCE(blob_fees, 0) + COALESCE(priority_fee_usd, 0) AS fees
    , revenue AS burned_fee_allocation

    -- Financial Statements
    , revenue_native + COALESCE(blob_fees_native, 0) AS revenue_native
    , revenue + COALESCE(blob_fees, 0) AS revenue
    , block_rewards_native * price AS token_incentives
    , revenue - token_incentives AS earnings

    -- Developer Data
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , weekly_contracts_deployed
    , weekly_contract_deployers

    -- Stablecoin Data
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
    , stablecoin_data.p2p_stablecoin_transfer_volume

    -- ETF Data
    , net_etf_flow_native
    , net_etf_flow
    , cumulative_etf_flow_native
    , cumulative_etf_flow

    -- Supply Data
    , block_rewards_native AS gross_emissions_native
    , block_rewards_native * price AS gross_emissions
    , eth_supply.issued_supply AS issued_supply_native
    , eth_supply.circulating_supply AS circulating_supply_native

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
LEFT JOIN stablecoin_data USING (date)
LEFT JOIN censored_block_metrics USING (date)
LEFT JOIN staking_data USING (date)
LEFT JOIN revenue_data USING (date)
LEFT JOIN validator_queue_data USING (date)
LEFT JOIN github_data USING (date)
LEFT JOIN contract_data USING (date)
LEFT JOIN nft_metrics USING (date)
LEFT JOIN p2p_metrics USING (date)
LEFT JOIN rolling_metrics USING (date)
LEFT JOIN da_metrics USING (date)
LEFT JOIN etf_metrics USING (date)
LEFT JOIN ethereum_dex_volumes USING (date)
LEFT JOIN block_rewards_data USING (date)
LEFT JOIN eth_supply USING (date)
LEFT JOIN adjusted_dau_metrics USING (date)
LEFT JOIN application_fees USING (date)
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
AND date_spine.date < to_date(sysdate())
