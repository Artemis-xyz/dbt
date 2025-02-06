{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='raw',
    )
}}

WITH trading_metrics AS (
        SELECT 
            block_date AS date,
            balancer_pool_address as pool_address,
            COUNT(*) AS swap_count,
            SUM(swap_fee_usd) AS trading_fees,
            SUM(swap_fee_usd) AS fees, --total fees == trading fees 
            SUM(swap_fee_usd) AS primary_supply_side_revenue,
            0 AS secondary_supply_side_revenue,
            SUM(swap_fee_usd) AS total_supply_side_revenue,
            0 AS protocol_revenue,
            0 AS operating_expenses,
            0 AS token_incentives,        -- to verify
            0 AS protocol_earnings,       -- to verify
            SUM(token_sold_amount_usd) AS trading_volume,
            COUNT(DISTINCT taker) AS unique_traders
        FROM {{ ref('fact_balancer_v1_swaps') }}
        WHERE NOT (token_sold_amount_raw > 9E25 AND token_sold_amount_usd > 10000000000) --filter out deprecated, outlier tokens
        GROUP BY block_date, pool_address
),
date_spine as (
    select date
    from {{ ref('dim_date_spine') }}
    where date between '2020-03-01' and to_date(sysdate())
)
SELECT
    date_spine.date,
    trading_metrics.pool_address,
    '1' as version,
    trading_metrics.swap_count,
    trading_metrics.trading_fees,
    trading_metrics.fees,
    trading_metrics.primary_supply_side_revenue,
    trading_metrics.secondary_supply_side_revenue,
    trading_metrics.total_supply_side_revenue,
    trading_metrics.protocol_revenue,
    trading_metrics.operating_expenses,
    trading_metrics.token_incentives,
    trading_metrics.protocol_earnings,
    trading_metrics.trading_volume,
    trading_metrics.unique_traders,
    tvl_balancer_v1.tvl_usd,
    tvl_balancer_v1.tvl_usd as net_deposits
FROM date_spine
left join trading_metrics using (date)
left join tvl_balancer_v1 using (date)