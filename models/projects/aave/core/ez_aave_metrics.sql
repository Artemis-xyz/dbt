{{
    config(
        materialized="incremental",
        snowflake_warehouse="AAVE",
        database="aave",
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
        WHERE date >= '2020-12-03' AND date < to_date(sysdate())
    )
    , deposits_borrows_lender_revenue AS (
        SELECT * FROM {{ref("fact_aave_v3_arbitrum_deposits_borrows_lender_revenue")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v2_avalanche_deposits_borrows_lender_revenue")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_avalanche_deposits_borrows_lender_revenue")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_base_deposits_borrows_lender_revenue")}}
        UNION ALL 
        SELECT * FROM {{ref("fact_aave_v3_bsc_deposits_borrows_lender_revenue")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v2_ethereum_deposits_borrows_lender_revenue")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_ethereum_deposits_borrows_lender_revenue")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_gnosis_deposits_borrows_lender_revenue")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_optimism_deposits_borrows_lender_revenue")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v2_polygon_deposits_borrows_lender_revenue")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_polygon_deposits_borrows_lender_revenue")}}
    )
    , aave_outstanding_supply_net_deposits_deposit_revenue AS (
        SELECT
            date
            , SUM(borrows_usd) as outstanding_supply
            , SUM(supply_usd) as net_deposits
            , net_deposits - outstanding_supply as tvl
            , SUM(deposit_revenue) as supply_side_deposit_revenue
            , SUM(interest_rate_fees) as interest_rate_fees
            , SUM(reserve_factor_revenue) as reserve_factor_revenue
        FROM deposits_borrows_lender_revenue
        GROUP BY 1
    )
    , flashloan_fees AS (
        SELECT * FROM {{ref("fact_aave_v3_arbitrum_flashloan_fees")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v2_avalanche_flashloan_fees")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_avalanche_flashloan_fees")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_base_flashloan_fees")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v2_ethereum_flashloan_fees")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_ethereum_flashloan_fees")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_gnosis_flashloan_fees")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_optimism_flashloan_fees")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v2_polygon_flashloan_fees")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_polygon_flashloan_fees")}}
    )
    , aave_flashloan_fees AS (
        SELECT 
            date
            , SUM(amount_usd) AS flashloan_fees
        FROM flashloan_fees
        GROUP BY 1
    )
    , liquidation_revenue AS (
        SELECT * FROM {{ref("fact_aave_v3_arbitrum_liquidation_revenue")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v2_avalanche_liquidation_revenue")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_avalanche_liquidation_revenue")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_base_liquidation_revenue")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_bsc_liquidation_revenue")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v2_ethereum_liquidation_revenue")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_ethereum_liquidation_revenue")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_gnosis_liquidation_revenue")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_optimism_liquidation_revenue")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v2_polygon_liquidation_revenue")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_polygon_liquidation_revenue")}}
    )
    , aave_liquidation_supply_side_revenue AS (
        SELECT 
            date
            , SUM(liquidation_revenue) AS liquidation_revenue
        FROM liquidation_revenue
        GROUP BY 1
    )
    , ecosystem_incentives AS (
        SELECT * FROM {{ref("fact_aave_v3_arbitrum_ecosystem_incentives")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v2_avalanche_ecosystem_incentives")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_avalanche_ecosystem_incentives")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_base_ecosystem_incentives")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_bsc_ecosystem_incentives")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v2_ethereum_ecosystem_incentives")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_ethereum_ecosystem_incentives")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_gnosis_ecosystem_incentives")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_optimism_ecosystem_incentives")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v2_polygon_ecosystem_incentives")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v3_polygon_ecosystem_incentives")}}
    )
    , aave_treasury AS (
        SELECT * FROM {{ref("fact_aave_aavura_treasury")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_v2_collector")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_safety_module")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_ecosystem_reserve")}}
    )
    , treasury AS (
        SELECT
            date
            , SUM(CASE WHEN LOWER(token_address) = LOWER('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9') THEN amount_usd ELSE 0 END) AS treasury_value_native
            , SUM(amount_usd) AS treasury_value
        FROM aave_treasury
        GROUP BY 1
    )
    , aave_net_treasury AS (
        SELECT * FROM {{ref("fact_aave_v2_collector")}}
        UNION ALL
        SELECT * FROM {{ref("fact_aave_aavura_treasury")}}
    )
    , net_treasury_data AS (
        SELECT
            date
            , SUM(amount_usd) AS net_treasury_value
        FROM aave_net_treasury
        GROUP BY 1
    )
    , aave_ecosystem_incentives AS (
        SELECT 
            date
            , SUM(amount_usd) AS ecosystem_incentives
        FROM ecosystem_incentives
        GROUP BY 1
    )
    , dao_trading_revenue AS (
        SELECT
            date
            , SUM(trading_fees_usd) AS trading_fees
        FROM {{ ref("fact_aave_dao_balancer_trading_fees")}}
        GROUP BY 1
    )
    , safety_incentives AS (
        SELECT
            date
            , SUM(amount_usd) AS safety_incentives
        FROM {{ ref("fact_aave_dao_safety_incentives")}}
        GROUP BY 1
    )
    , gho_treasury_revenue AS (
        SELECT
            date
            , SUM(amount_usd) AS gho_revenue
        FROM {{ ref("fact_aave_gho_treasury_revenue")}}
        GROUP BY 1
    )

    , flashloan_fees_to_protocol as (
        select
             date,
            protocol_revenue
        from {{ ref('fact_aave_flashloan_fees') }}
    )

    , issued_supply_metrics as (
        select 
            date,
            max_supply as max_supply_native,
            total_supply_to_date as total_supply_native,
            issued_supply as issued_supply_native,
            circulating_supply as circulating_supply_native
        from {{ ref('fact_aave_issued_supply_and_float') }}
    )

    , aave_token_holders as (
        select
            date
            , token_holder_count
        FROM {{ ref("fact_aave_token_holders")}}
    )
    , market_data AS (
        SELECT 
            date
            , shifted_token_price_usd as price
            , shifted_token_h24_volume_usd as h24_volume
            , shifted_token_market_cap as market_cap
            , t2.total_supply * price as fdmc
            , shifted_token_h24_volume_usd / market_cap as token_turnover_circulating
            , shifted_token_h24_volume_usd / fdmc as token_turnover_fdv
        FROM {{ ref("fact_coingecko_token_date_adjusted_gold") }} t1
        INNER JOIN
            (
                SELECT
                    token_id
                    , coalesce(token_max_supply, token_total_supply) AS total_supply
                FROM {{ ref("fact_coingecko_token_realtime_data") }}
                WHERE token_id = 'aave'
            ) t2
            on t1.coingecko_id = t2.token_id
        WHERE
            coingecko_id = 'aave'
            AND date < dateadd(day, -1, to_date(sysdate()))
    )
SELECT
    date_spine.date
    , 'aave' AS artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Data
    , outstanding_supply as lending_loans
    , net_deposits as lending_deposits
    , outstanding_supply + tvl AS lending_loan_capacity
    , tvl AS lending_tvl
    , tvl AS tvl 
    , token_holder_count

    -- Fee Data
    , interest_rate_fees AS interest_rate_fees
    , supply_side_deposit_revenue AS deposit_fees
    , flashloan_fees
    , gho_revenue AS gho_fees
    , trading_fees AS dao_trading_revenue
    , liquidation_revenue AS liquidator_fees
    , reserve_factor_revenue AS reserve_factor_fees
    , coalesce(interest_rate_fees, 0) 
        + coalesce(supply_side_deposit_revenue, 0)
        + coalesce(flashloan_fees, 0) 
        + coalesce(gho_revenue, 0) 
        + coalesce(trading_fees, 0) 
        + coalesce(liquidation_revenue, 0) 
        + coalesce(reserve_factor_revenue, 0)
    AS fees
    , deposit_fees + flashloan_fees AS lp_fee_allocation
    , dao_trading_revenue AS dao_fee_allocation
    , gho_revenue + reserve_factor_revenue AS treasury_fee_allocation
    , liquidation_revenue AS liquidator_fee_allocation

    -- Financial Statements
    , coalesce(reserve_factor_revenue, 0) + coalesce(dao_trading_revenue, 0) + coalesce(gho_revenue, 0) AS revenue
    , coalesce(ecosystem_incentives, 0) + coalesce(safety_incentives, 0) AS token_incentives
    , revenue - token_incentives AS earnings

    -- Treasury Data
    , treasury_value as treasury
    , net_treasury_value as net_treasury

    -- Turnover Metrics
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
FROM date_spine
LEFT JOIN aave_outstanding_supply_net_deposits_deposit_revenue USING (date)
LEFT JOIN aave_flashloan_fees USING (date)
LEFT JOIN aave_liquidation_supply_side_revenue USING (date)
LEFT JOIN aave_ecosystem_incentives USING (date)
LEFT JOIN dao_trading_revenue USING (date)
LEFT JOIN safety_incentives USING (date)
LEFT JOIN gho_treasury_revenue USING (date)
LEFT JOIN treasury USING (date)
LEFT JOIN net_treasury_data USING (date)
LEFT JOIN aave_token_holders USING (date)
LEFT JOIN market_data USING (date)
WHERE true
{{ ez_metrics_incremental("aave_outstanding_supply_net_deposits_deposit_revenue.date", backfill_date) }}
AND aave_outstanding_supply_net_deposits_deposit_revenue.date < to_date(sysdate())