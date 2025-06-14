{{
    config(
        materialized="table",
        snowflake_warehouse="MOMENTUM",
        database="momentum",
        schema="core",
        alias="ez_metrics_by_token",
    )
}}

WITH
    spot_volumes AS(
        SELECT
            date, 
            LOWER(symbol_a) AS token, 
            SUM(amount_a_swapped_native) AS volume_native, 
            SUM(amount_a_swapped_usd) AS volume_usd,
            SUM(GREATEST(COALESCE(amount_a_swapped_usd, 0), COALESCE(amount_b_swapped_usd, 0))) AS spot_volume
        FROM {{ ref("fact_momentum_spot_volume") }}
        GROUP BY 1, 2

        UNION ALL
        
        SELECT
            date, 
            LOWER(symbol_b) AS token, 
            SUM(amount_b_swapped_native) AS volume_native, 
            SUM(amount_b_swapped_usd) AS volume_usd,
            SUM(GREATEST(COALESCE(amount_a_swapped_usd, 0), COALESCE(amount_b_swapped_usd, 0))) AS spot_volume
        FROM {{ ref("fact_momentum_spot_volume") }}
        GROUP BY 1, 2

    )

    , spot_dau_txns AS (
        SELECT 
            date, 
            LOWER(token_sold) AS token, 
            SUM(pool_dau) AS dau, 
            SUM(pool_txns) AS txns
        FROM {{ ref("fact_momentum_spot_dau_txns") }}
        GROUP BY 1, 2

        UNION ALL

        SELECT 
            date, 
            LOWER(token_bought) AS token, 
            SUM(pool_dau) AS dau, 
            SUM(pool_txns) AS txns
        FROM {{ ref("fact_momentum_spot_dau_txns") }}
        GROUP BY 1, 2

    )

    , spot_fees_revenue AS (
        SELECT 
            date, 
            LOWER(symbol_a) AS token, 
            SUM(fees_native) AS fees_native, 
            SUM(fees) AS fees, 
            SUM(service_fee_allocation) AS service_fee_allocation, 
            SUM(service_fee_allocation_native) AS service_fee_allocation_native, 
            SUM(foundation_fee_allocation) AS foundation_fee_allocation, 
            SUM(foundation_fee_allocation_native) AS foundation_fee_allocation_native,
        FROM {{ ref("fact_momentum_spot_fees_revenue") }}
        GROUP BY 1, 2

        UNION ALL

        SELECT
            date, 
            LOWER(symbol_b) AS token, 
            SUM(fees_native) AS fees_native, 
            SUM(fees) AS fees, 
            SUM(service_fee_allocation) AS service_fee_allocation, 
            SUM(service_fee_allocation_native) AS service_fee_allocation_native, 
            SUM(foundation_fee_allocation) AS foundation_fee_allocation,  
            SUM(foundation_fee_allocation_native) AS foundation_fee_allocation_native
        FROM {{ ref("fact_momentum_spot_fees_revenue") }}
        GROUP BY 1, 2
    )

    , tvl AS (
        SELECT 
            date, 
            LOWER(symbol_a) AS token, 
            SUM(vault_a_amount_native) AS tvl_native, 
            SUM(vault_a_amount_usd) AS tvl,
        FROM {{ ref("fact_momentum_spot_tvl") }}
        GROUP BY 1, 2

        UNION ALL

        SELECT 
            date, 
            LOWER(symbol_b) AS token, 
            SUM(vault_b_amount_native) AS tvl_native, 
            SUM(vault_b_amount_usd) AS tvl,
        FROM {{ ref("fact_momentum_spot_tvl") }}
        GROUP BY 1, 2
    )

    SELECT
        spot_volumes.date, 
        LOWER(spot_volumes.token) AS token, 
        SUM(spot_volumes.volume_native) AS spot_volume_native, 
        SUM(spot_volumes.volume_usd) AS spot_volume,
        SUM(spot_dau_txns.dau) AS spot_dau, 
        SUM(spot_dau_txns.txns) AS spot_txns, 
        SUM(spot_fees_revenue.fees_native) AS fees_native, 
        SUM(spot_fees_revenue.fees) AS fees, 
        SUM(spot_fees_revenue.service_fee_allocation) AS service_fee_allocation, 
        SUM(spot_fees_revenue.service_fee_allocation_native) AS service_fee_allocation_native, 
        SUM(spot_fees_revenue.foundation_fee_allocation) AS foundation_fee_allocation, 
        SUM(spot_fees_revenue.foundation_fee_allocation_native) AS foundation_fee_allocation_native, 
        SUM(tvl.tvl_native) AS tvl_native, 
        SUM(tvl.tvl) AS tvl
    FROM spot_volumes
    LEFT JOIN spot_dau_txns ON spot_volumes.date = spot_dau_txns.date AND LOWER(spot_volumes.token) = LOWER(spot_dau_txns.token)
    LEFT JOIN spot_fees_revenue ON spot_volumes.date = spot_fees_revenue.date AND LOWER(spot_volumes.token) = LOWER(spot_fees_revenue.token)
    LEFT JOIN tvl ON spot_volumes.date = tvl.date AND LOWER(spot_volumes.token) = LOWER(tvl.token)
    GROUP BY spot_volumes.date, LOWER(spot_volumes.token)




