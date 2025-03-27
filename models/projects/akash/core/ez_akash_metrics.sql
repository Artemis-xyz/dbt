{{
    config(
        materialized="table",
        snowflake_warehouse="AKASH",
        database="akash",
        schema="core",
        alias="ez_metrics",
    )
}}

WITH
active_providers AS (
    SELECT * FROM {{ ref("fact_akash_active_providers_silver") }}
)

, active_leases AS (SELECT * FROM {{ ref("fact_akash_active_leases_silver") }})

, new_leases AS (SELECT * FROM {{ ref("fact_akash_new_leases_silver") }})

, compute_fees_native AS (
    SELECT * FROM {{ ref("fact_akash_compute_fees_native_silver") }}
)

, compute_fees_usdc AS (
    SELECT * FROM {{ ref("fact_akash_compute_fees_usdc_silver") }}
)

, compute_fees_total_usd AS (
    SELECT * FROM {{ ref("fact_akash_compute_fees_total_usd_silver") }}
)

, validator_fees_native AS (
    SELECT * FROM {{ ref("fact_akash_validator_fees_native_silver") }}
)

, validator_fees AS (SELECT * FROM {{ ref("fact_akash_validator_fees_silver") }}
)

, total_fees AS (SELECT * FROM {{ ref("fact_akash_total_fees_silver") }})

, revenue AS (SELECT * FROM {{ ref("fact_akash_revenue_silver") }})

, mints AS (SELECT * FROM {{ ref("fact_akash_mints_silver") }})

, burns AS (SELECT * FROM {{ ref("fact_akash_burns_native_silver") }})


SELECT
    mints.date
    , 'akash' AS chain
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
    , coalesce(mints.mints, 0) AS mints

    -- Standardized Metrics
    , (coalesce(compute_fees_total_usd.compute_fees_total_usd, 0))/ 1e6 AS compute_fees
    , coalesce(validator_fees.validator_fees, 0) AS gas_fees
    , service_fees + validator_fees AS ecosystem_revenue
    , validator_fees AS validator_revenue
    , revenue.revenue AS treasury_revenue
    , service_fees - treasury_revenue AS service_revenue
    , coalesce(burns.total_burned_native, 0) AS burns_native
    , coalesce(mints.mints, 0) AS mints_native
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
WHERE mints.date < to_date(sysdate())
ORDER BY date DESC
