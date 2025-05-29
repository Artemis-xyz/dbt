{{
    config(
        materialized="table",
        database = 'orca',
        schema = 'core',
        snowflake_warehouse = 'ORCA',
        alias = 'ez_metrics_by_chain'
    )
}}

select
    date
    , 'solana' as chain
    , trading_volume
    , fees
    , revenue
    , total_supply_side_revenue
    , number_of_swaps
    , unique_traders
    , trading_fees

    -- Standardized Metrics
    -- Usage/Sector Metrics
    , spot_dau
    , spot_txns
    , spot_volume
    , tvl

    -- Money Metrics
    , treasury_cash_flow
    , ecosystem_revenue
    , service_cash_flow
    , other_cash_flow
from {{ ref("ez_orca_metrics") }}