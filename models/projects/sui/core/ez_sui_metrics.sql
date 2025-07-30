{{
    config(
        materialized="incremental",
        snowflake_warehouse="SUI",
        database="sui",
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

WITH
    date_spine AS (
        SELECT date
        FROM {{ ref("dim_date_spine") }}
        WHERE date >= '2023-04-12'
        AND date < to_date(sysdate())
    )
    , fundamental_data AS (
        SELECT 
            * EXCLUDE date
            , TO_TIMESTAMP_NTZ(date) AS date 
        FROM {{ source('PROD_LANDING', 'ez_sui_metrics') }}
    )
    , market_data AS ({{ get_coingecko_metrics("sui") }})
    , defillama_data AS ({{ get_defillama_metrics("sui") }})
    , stablecoin_data AS ({{ get_stablecoin_metrics("sui") }})
    , github_data AS ({{ get_github_metrics("sui") }})
    , supply_data AS (
        SELECT 
            date
            , max_supply_native
            , total_supply_native
            , foundation_owned_supply_native
            , unvested_tokens_native
            , gross_emissions_native
        FROM {{ ref("fact_sui_supply_data") }}
    )
SELECT
    fundamental_data.date
    , 'sui' AS artemis_id

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
    , tvl AS chain_tvl
    , tvl
    , dex_volumes AS chain_spot_volume
    , avg_txn_fee AS chain_avg_txn_fee
    , returning_users
    , new_users

    -- Fee Data
    , fees_native
    , fees AS chain_fees
    , fees 
    , revenue AS burned_fee_allocation

    -- Financial Statements
    , revenue_native
    , revenue

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
    , p2p_stablecoin_transfer_volume

    -- Developer Data
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem

    -- Supply Data
    , max_supply_native
    , total_supply_native
    , gross_emissions_native
    , total_supply_native - foundation_owned_supply_native - burned_fee_allocation_native as issued_supply_native
    , total_supply_native - foundation_owned_supply_native - burned_fee_allocation_native - unvested_tokens_native as circulating_supply_native

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
LEFT JOIN github_data USING (date)
LEFT JOIN supply_data USING (date)
WHERE true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
AND date_spine.date < to_date(sysdate())
GROUP BY ALL