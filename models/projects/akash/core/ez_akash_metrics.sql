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
active_providers AS (
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
, price as ({{ get_coingecko_metrics("akash-network") }})

, premine_unlocks AS (
    SELECT * 
    FROM {{ ref("fact_akash_premine_unlocks") }}
)

SELECT
    mints.date
    , coalesce(active_leases.active_leases, 0) AS active_leases
    , coalesce(active_providers.active_providers, 0) AS active_providers
    , coalesce(new_leases.new_leases, 0) AS new_leases
    , (coalesce(compute_fees_native.compute_fees_native, 0))
    / 1e6 AS compute_fees_native
    , (coalesce(compute_fees_usdc.compute_fees_usdc, 0))
    / 1e6 AS compute_fees_usdc
    , (coalesce(compute_fees_total_usd.compute_fees_total_usd, 0))
    / 1e6 AS compute_fees_total_usd
    , coalesce(validator_fees_native.validator_fees_native, 0)
        AS validator_fees_native
    , coalesce(validator_fees.validator_fees, 0) AS validator_fees
    , coalesce(total_fees.total_fees, 0) AS total_fees
    , coalesce(revenue.revenue, 0) AS revenue
    , coalesce(burns.total_burned_native, 0) AS total_burned_native

    -- Standardized Metrics

    -- Token Metrics
    , coalesce(price, 0) as price
    , coalesce(market_cap, 0) as market_cap
    , coalesce(fdmc, 0) as fdmc
    , coalesce(token_volume, 0) as token_volume  

    -- Cashflow Metrics
    , (coalesce(compute_fees_total_usd.compute_fees_total_usd, 0))/ 1e6 AS compute_fees
    , coalesce(validator_fees.validator_fees, 0) AS gas_fees
    , compute_fees + gas_fees AS fees
    , validator_fees AS validator_fee_allocation
    , revenue.revenue AS treasury_fee_allocation
    , compute_fees - treasury_fee_allocation AS service_fee_allocation
    , coalesce(burns.total_burned_native, 0) AS burns_native

    -- Supply Metrics
    , coalesce(mints.mints, 0) AS gross_emissions_native
    , coalesce(premine_unlocks.pre_mine_unlocks, 0) AS premine_unlocks_native
    , coalesce(mints.mints, 0) + coalesce(premine_unlocks.pre_mine_unlocks, 0) - coalesce(burns.total_burned_native, 0) AS net_supply_change_native
    , sum((coalesce(mints.mints, 0) - coalesce(burns.total_burned_native, 0)) + coalesce(premine_unlocks.pre_mine_unlocks, 0)) OVER (order by mints.date) AS circulating_supply_native
    
    -- Turnover Metrics
    , coalesce(token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(token_turnover_fdv, 0) as token_turnover_fdv 

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM mints
LEFT JOIN active_providers ON mints.date = active_providers.date
LEFT JOIN new_leases ON mints.date = new_leases.date
LEFT JOIN compute_fees_native ON mints.date = compute_fees_native.date
LEFT JOIN compute_fees_usdc ON mints.date = compute_fees_usdc.date
LEFT JOIN compute_fees_total_usd ON mints.date = compute_fees_total_usd.date
LEFT JOIN validator_fees_native ON mints.date = validator_fees_native.date
LEFT JOIN validator_fees ON mints.date = validator_fees.date
LEFT JOIN total_fees ON mints.date = total_fees.date
LEFT JOIN revenue ON mints.date = revenue.date
LEFT JOIN burns ON mints.date = burns.date
LEFT JOIN active_leases ON mints.date = active_leases.date
LEFT JOIN price ON mints.date = price.date
LEFT JOIN premine_unlocks ON mints.date = premine_unlocks.date
WHERE true
{{ ez_metrics_incremental("mints.date", backfill_date) }}
and mints.date < to_date(sysdate())
ORDER BY date DESC
