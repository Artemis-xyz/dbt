{{
    config(
        materialized="incremental",
        snowflake_warehouse="AKASH",
        database="akash",
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
        WHERE date >= '2020-09-25' AND date < to_date(sysdate())
    )

    , active_providers AS (
        SELECT * 
        FROM {{ ref("fact_akash_active_providers_silver") }}
    )

    , active_leases AS (
        SELECT * 
        FROM {{ ref("fact_akash_active_leases_silver") }}
    )

    , new_leases AS (
        SELECT * 
        FROM {{ ref("fact_akash_new_leases_silver") }}
    )

    , compute_fees_native AS (
        SELECT * 
        FROM {{ ref("fact_akash_compute_fees_native_silver") }}
    )

    , compute_fees_usdc AS (
        SELECT * 
        FROM {{ ref("fact_akash_compute_fees_usdc_silver") }}
    )

    , compute_fees_total_usd AS (
        SELECT * 
        FROM {{ ref("fact_akash_compute_fees_total_usd_silver") }}
    )

    , validator_fees_native AS (
        SELECT * 
        FROM {{ ref("fact_akash_validator_fees_native_silver") }}
    )
    , validator_fees AS (
        SELECT * 
        FROM {{ ref("fact_akash_validator_fees_silver") }}
    )

    , total_fees AS (
        SELECT * 
        FROM {{ ref("fact_akash_total_fees_silver") }}
    )

    , revenue AS (
        SELECT * 
        FROM {{ ref("fact_akash_revenue_silver") }}
    )

    , mints AS (
        SELECT * 
        FROM {{ ref("fact_akash_mints_silver") }}
    )

    , burns AS (
        SELECT * 
        FROM {{ ref("fact_akash_burns_native_silver") }}
    )
    , market_data AS ({{ get_coingecko_metrics("akash-network") }})

    , premine_unlocks AS (
        SELECT * 
        FROM {{ ref("fact_akash_premine_unlocks") }}
    )

SELECT
    mints.date
    , 'akash' AS artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume  

    -- Fee Data
    , (coalesce(compute_fees_total_usd.compute_fees_total_usd, 0))/ 1e6 AS compute_fees
    , coalesce(validator_fees.validator_fees, 0) AS gas_fees
    , compute_fees + gas_fees AS fees
    , validator_fees AS validator_fee_allocation
    , revenue.revenue AS treasury_fee_allocation
    , compute_fees - treasury_fee_allocation AS other_fee_allocation

    -- Financial Statements
    , revenue.revenue + burns.total_burned_native AS revenue
    , revenue.revenue AS earnings

    -- Supply Data
    , coalesce(mints.mints, 0) AS gross_emissions_native
    , coalesce(premine_unlocks.pre_mine_unlocks, 0) AS premine_unlocks_native
    , coalesce(burns.total_burned_native, 0) AS burns
    , sum((coalesce(mints.mints, 0) - coalesce(burns.total_burned_native, 0)) + coalesce(premine_unlocks.pre_mine_unlocks, 0)) OVER (order by mints.date) AS circulating_supply_native
    
    -- Turnover Metrics
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv 

    -- Bespoke Metrics
    , COALESCE(active_leases.active_leases, 0) AS active_leases
    , COALESCE(active_providers.active_providers, 0) AS active_providers
    , COALESCE(new_leases.new_leases, 0) AS new_leases
    , (COALESCE(compute_fees_native.compute_fees_native, 0))
    / 1e6 AS compute_fees_native
    , (COALESCE(compute_fees_usdc.compute_fees_usdc, 0))
    / 1e6 AS compute_fees_usdc
    , (COALESCE(compute_fees_total_usd.compute_fees_total_usd, 0))
    / 1e6 AS compute_fees_total_usd

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM date_spine
LEFT JOIN mints USING (date)
LEFT JOIN active_providers USING (date)
LEFT JOIN new_leases USING (date)
LEFT JOIN compute_fees_native USING (date)
LEFT JOIN compute_fees_usdc USING (date)
LEFT JOIN compute_fees_total_usd USING (date)
LEFT JOIN validator_fees_native USING (date)
LEFT JOIN validator_fees USING (date)
LEFT JOIN total_fees USING (date)
LEFT JOIN revenue USING (date)
LEFT JOIN burns USING (date)
LEFT JOIN active_leases USING (date)
LEFT JOIN market_data USING (date)
LEFT JOIN premine_unlocks USING (date)
WHERE true
{{ ez_metrics_incremental("mints.date", backfill_date) }}
AND mints.date < to_date(sysdate())
ORDER BY date DESC
