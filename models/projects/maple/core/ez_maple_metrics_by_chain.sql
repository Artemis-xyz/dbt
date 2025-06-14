{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'MAPLE',
        database = 'MAPLE',
        schema = 'core',
        alias = 'ez_metrics_by_chain'
    )
}}

SELECT
    date,
    'ethereum' as chain,
    interest_fees,
    primary_supply_side_revenue,
    total_supply_side_revenue,
    revenue,
    token_incentives,
    token_incentives_native,
    total_expenses,
    earnings,
    treasury_value,
    treasury_value_native,
    net_treasury_value,
    tvl,
    net_deposits,
    outstanding_supply,
    price,
    market_cap,
    fdmc,
    token_turnover_circulating,
    token_turnover_fdv,
    token_volume,
    token_holder_count
FROM {{ ref('ez_maple_metrics') }}