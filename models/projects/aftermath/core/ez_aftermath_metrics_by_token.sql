{{
    config(
        materialized="table",
        snowflake_warehouse="AFTERMATH",
        database="aftermath",
        schema="core",
        alias="ez_metrics_by_token",
    )
}}

WITH
    spot_volumes AS(
        SELECT
            date, symbol_a AS token, SUM(amount_a_swapped_native) AS volume_native, SUM(amount_a_swapped_usd) AS volume_usd
        FROM {{ ref("fact_aftermath_spot_volumes") }}
        GROUP BY 1, 2

        UNION ALL
        
        SELECT
            date, symbol_b AS token, SUM(amount_b_swapped_native) AS volume_native, SUM(amount_b_swapped_usd) AS volume_usd
        FROM {{ ref("fact_aftermath_spot_volumes") }}
        GROUP BY 1, 2

    )

    , spot_dau_txns AS (
        SELECT date, token_sold AS token, SUM(pool_dau) AS dau, SUM(pool_txns) AS txns
        FROM {{ ref("fact_aftermath_spot_dau_txns") }}
        GROUP BY 1, 2

        UNION ALL

        SELECT date, token_bought AS token, SUM(pool_dau) AS dau, SUM(pool_txns) AS txns
        FROM {{ ref("fact_aftermath_spot_dau_txns") }}
        GROUP BY 1, 2

    )

    , spot_fees_revenue AS (
        SELECT 
            date,           
            symbol_a AS token, 
            SUM(fees_native) AS fees_native, 
            SUM(fees) AS fees, 
            SUM(service_cash_flow_native) AS service_cash_flow_native, 
            SUM(service_cash_flow) AS service_cash_flow, 
            SUM(foundation_cash_flow_native) AS foundation_cash_flow_native, 
            SUM(foundation_cash_flow) AS foundation_cash_flow
        FROM {{ ref("fact_aftermath_spot_fees_revenues") }}
        GROUP BY 1, 2

        UNION ALL

        SELECT 
            date, 
            symbol_b AS token, 
            SUM(fees_native) AS fees_native, 
            SUM(fees) AS fees, 
            SUM(service_cash_flow_native) AS service_cash_flow_native, 
            SUM(service_cash_flow) AS service_cash_flow, 
            SUM(foundation_cash_flow_native) AS foundation_cash_flow_native, 
            SUM(foundation_cash_flow) AS foundation_cash_flow
        FROM {{ ref("fact_aftermath_spot_fees_revenues") }}
        GROUP BY 1, 2
    )

    , tvl AS (
        SELECT date, symbol_a AS token, SUM(vault_a_amount_native) AS tvl_native, SUM(vault_a_amount_usd) AS tvl
        FROM {{ ref("fact_aftermath_spot_tvl") }}
        GROUP BY 1, 2

        UNION ALL

        SELECT date, symbol_b AS token, SUM(vault_b_amount_native) AS tvl_native, SUM(vault_b_amount_usd) AS tvl
        FROM {{ ref("fact_aftermath_spot_tvl") }}
        GROUP BY 1, 2
    )

    SELECT
        spot_volumes.date, 
        spot_volumes.token, 
        SUM(spot_volumes.volume_native) AS spot_volume_native, 
        SUM(spot_volumes.volume_usd) AS spot_volume,
        SUM(spot_dau_txns.dau) AS spot_dau, 
        SUM(spot_dau_txns.txns) AS spot_txns, 
        SUM(spot_fees_revenue.fees) AS spot_fees, 
        SUM(spot_fees_revenue.service_cash_flow) AS service_cash_flow, 
        SUM(spot_fees_revenue.foundation_cash_flow) AS foundation_cash_flow,
        SUM(spot_fees_revenue.fees_native) AS spot_fees_native, 
        SUM(spot_fees_revenue.service_cash_flow_native) AS service_cash_flow_native, 
        SUM(spot_fees_revenue.foundation_cash_flow_native) AS foundation_cash_flow_native, 
        SUM(tvl.tvl_native) AS tvl_native, 
        SUM(tvl.tvl) AS tvl
    FROM spot_volumes
    LEFT JOIN spot_dau_txns ON spot_volumes.date = spot_dau_txns.date AND lower(spot_volumes.token) = lower(spot_dau_txns.token)
    LEFT JOIN spot_fees_revenue ON spot_volumes.date = spot_fees_revenue.date AND lower(spot_volumes.token) = lower(spot_fees_revenue.token)
    LEFT JOIN tvl ON spot_volumes.date = tvl.date AND lower(spot_volumes.token) = lower(tvl.token)
    GROUP BY 1, 2




