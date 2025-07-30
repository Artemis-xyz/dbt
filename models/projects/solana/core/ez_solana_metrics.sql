-- depends_on {{ ref('fact_solana_transactions_v2') }}
{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="SOLANA",
        database="solana",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

WITH
    date_spine AS (
        SELECT date
        FROM {{ ref("dim_date_spine") }}
        WHERE date >= '2020-10-07' AND date < TO_DATE(SYSDATE())
    )
    , contract_data AS ({{ get_contract_metrics("solana") }})
    , stablecoin_data AS ({{ get_stablecoin_metrics("solana") }})
    , defillama_data AS ({{ get_defillama_metrics("solana") }})
    , github_data AS ({{ get_github_metrics("solana") }})
    , market_data AS ({{ get_coingecko_metrics("solana") }})
    , staking_data AS ({{ get_staking_metrics("solana") }})
    , issuance_data AS (
        SELECT date, chain, issuance 
        FROM {{ ref("fact_solana_issuance_silver") }}
    )
    , nft_metrics AS ({{ get_nft_metrics("solana") }})
    , p2p_metrics AS ({{ get_p2p_metrics("solana") }})
    , rolling_metrics AS ({{ get_rolling_active_address_metrics("solana") }})
    , fundamental_usage AS (
        SELECT
            date,
            gas,
            gas_usd,
            median_txn_fee,
            base_fee_native,
            txns,
            dau,
            returning_users,
            new_users,
            vote_tx_fee_native
        FROM {{ ref('fact_solana_fundamental_data') }}
    )
    , solana_dex_volumes AS (
        SELECT date, daily_volume_usd AS dex_volumes
        FROM {{ ref("fact_solana_dex_volumes") }}
    )
    , jito_tips AS (
        SELECT
            day as date,
            tip_fees
        FROM {{ ref('fact_jito_dau_txns_fees') }}
    )
    , supply_data AS (
        SELECT date, issued_supply, circulating_supply
        FROM {{ ref('fact_solana_supply_data') }}
    )
    , application_fees AS (
        SELECT 
            DATE_TRUNC(DAY, date) AS date 
            , SUM(COALESCE(fees, 0)) AS application_fees
        FROM {{ ref("ez_protocol_datahub_by_chain") }}
        WHERE chain = 'solana'
        -- excluding solana dex's to avoid double counting. It looks like these are included in dex_volumes above as dex_volumes defaults to the greater usd value of token_bought and token_sold
        -- this does not appear to be an issue for EVM based chains that rely on Dune dex.trades as that defaults to using the token_bought amount which is net of fees
            AND artemis_id NOT IN ('raydium', 'jupiter', 'saber', 'pumpfun')
        GROUP BY 1
    )

SELECT
    date_spine.date
    , 'solana' AS artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Data
    , dau
    , dau AS chain_dau
    , wau AS chain_wau
    , mau AS chain_mau
    , txns
    , txns AS chain_txns
    , tvl
    , tvl AS chain_tvl
    , returning_users
    , new_users
    , gas_usd / txns AS chain_avg_txn_fee
    , median_txn_fee AS chain_median_txn_fee
    , total_staked_native
    , total_staked_usd AS total_staked
    , nft_trading_volume AS chain_nft_trading_volume
    , p2p_native_transfer_volume
    , p2p_token_transfer_volume
    , p2p_transfer_volume
    , coalesce(solana_dex_volumes.dex_volumes, 0) + coalesce(nft_trading_volume, 0) + coalesce(p2p_transfer_volume, 0) as settlement_volume
    , solana_dex_volumes.dex_volumes as chain_spot_volume
        -- Blockworks' REV
    , chain_fees + COALESCE(jito_tips.tip_fees, 0) AS rev 
        -- TEA 
    , coalesce(rev, 0) + coalesce(settlement_volume, 0) + coalesce(application_fees.application_fees, 0) as total_economic_activity

    -- Fee Data
    , CASE
        WHEN (gas - base_fee_native) < 0.00001 THEN 0 ELSE (gas - base_fee_native)
    END AS priority_fees_native
    , gas + vote_tx_fee_native as fees_native
    , CASE
        WHEN (gas_usd - base_fee_native * price ) < 0.001 THEN 0 ELSE (gas_usd - base_fee_native * price )
    END AS priority_fees
    , base_fee_native * price AS base_fee
    , vote_tx_fee_native * price AS vote_tx_fee
    , gas_usd + vote_tx_fee_native * price AS chain_fees
    , gas_usd + vote_tx_fee_native * price AS fees
    , IFF(fundamental_usage.date < '2025-02-13', fees * .5, ((base_fee_native * price  + vote_tx_fee_native * price) * .5) + priority_fees) as validator_fee_allocation
    , IFF(fundamental_usage.date < '2025-02-13', fees * .5, (base_fee_native * price  + vote_tx_fee_native * price) * .5) as burned_fee_allocation

    -- Financial Statements
    , IFF(fundamental_usage.date < '2025-02-13', fees * .5, (base_fee_native * price  + vote_tx_fee_native * price) * .5) as revenue
    , issuance * price as token_incentives
    , revenue - token_incentives as earnings

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

    -- Supply Data
    , issuance AS gross_emissions_native
    , issuance * price AS gross_emissions
    , issued_supply as issued_supply_native
    , circulating_supply as circulating_supply_native

    -- Turnover Data
    , market_data.token_turnover_circulating 
    , market_data.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
FROM date_spine
LEFT JOIN fundamental_usage USING (date)
LEFT JOIN defillama_data USING (date)
LEFT JOIN stablecoin_data USING (date)
LEFT JOIN market_data USING (date)
LEFT JOIN github_data USING (date)
LEFT JOIN contract_data USING (date)
LEFT JOIN staking_data USING (date)
LEFT JOIN issuance_data USING (date)
LEFT JOIN nft_metrics USING (date)
LEFT JOIN p2p_metrics USING (date)
LEFT JOIN rolling_metrics USING (date)
LEFT JOIN solana_dex_volumes USING (date)
LEFT JOIN jito_tips USING (date)
LEFT JOIN supply_data USING (date)
LEFT JOIN application_fees USING (date)
WHERE true
{{ ez_metrics_incremental('fundamental_usage.date', backfill_date) }}
AND fundamental_usage.date < TO_DATE(SYSDATE())
