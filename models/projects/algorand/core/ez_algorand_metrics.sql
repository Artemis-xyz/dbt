{{
    config(
        materialized="incremental",
        snowflake_warehouse="ALGORAND",
        database="algorand",
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
        FROM {{ ref('dim_date_spine') }}
        WHERE date >= '2019-06-01' AND date < TO_DATE(SYSDATE())
    )
    -- Alternative fundamental source from BigQuery, preferred when possible over Snowflake data
    , fundamental_data AS (SELECT * FROM {{ ref('fact_algorand_fundamental_data') }})
    , cumulative_burns AS (
        WITH revenue AS (
            SELECT
                date,
                CASE 
                    WHEN date > '2024-12-31' THEN 0.5 * fees_native
                    ELSE fees_native
                END AS revenue_native
            FROM fundamental_data
        )
        SELECT
            date,
            SUM(revenue_native) OVER (ORDER BY date) AS cumulative_burns
        FROM revenue
        ORDER BY date
    )
    , supply_data AS (
        SELECT *, 
            10000000000 - cumulative_burns.cumulative_burns - unvested_supply AS issued_supply_native, 
            10000000000 - cumulative_burns.cumulative_burns - unvested_supply AS circulating_supply_native
        FROM {{ ref('fact_algorand_supply_data') }}
        LEFT JOIN cumulative_burns USING (date)
    )
    , market_data as (
        {{ get_coingecko_metrics("algorand") }}
    )
SELECT
    date_spine.date
    , 'algorand' as artemis_id
    
    -- Standardized Metrics

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Data
    , fundamental_data.dau AS chain_dau
    , fundamental_data.dau
    , fundamental_data.txns AS chain_txns
    , fundamental_data.txns

    -- Fee Data
    , fundamental_data.fees_native * market_data.price AS chain_fees
    , fundamental_data.fees_native * market_data.price AS fees
    , fundamental_data.rewards_algo * market_data.price AS validator_fee_allocation

    -- Financial Statements
    , CASE 
        WHEN date_spine.date > '2024-12-31' THEN 0.5 * fees
        ELSE fees
    END AS revenue

    -- Supply Data
    , COALESCE(supply_data.premine_unlocks, 0) AS premine_unlocks
    , supply_data.issued_supply_native
    , supply_data.circulating_supply_native

    -- Turnover Data
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv
    
    -- Bespoke metrics
    , fundamental_data.unique_eoas
    , fundamental_data.unique_senders
    , fundamental_data.unique_receivers
    , fundamental_data.new_eoas
    , fundamental_data.unique_pairs
    , fundamental_data.unique_eoa_pairs
    , fundamental_data.unique_tokens

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM date_spine
LEFT JOIN fundamental_data USING (date)
LEFT JOIN market_data USING (date)
LEFT JOIN supply_data USING (date)
LEFT JOIN cumulative_burns USING (date)WHERE true
{{ ez_metrics_incremental("fundamental_data.date", backfill_date) }}
and fundamental_data.date < to_date(sysdate())