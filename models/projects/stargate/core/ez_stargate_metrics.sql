{{
    config(
        materialized="table",
        snowflake_warehouse="STARGATE",
        database="stargate",
        schema="core",
        alias="ez_metrics",
    )
}}

SELECT 
    date,
    'stargate' as chain,
    txns,
    avg_txn_size,
    bridge_volume,
    dau,
    new_addresses,
    returning_addresses,
    cumulative_addresses,
    daily_growth_pct,
    protocol_treasury_fee,
    vestg_fee,
    lp_fee,
    supply_side_fee,
    revenue,
    fees,
    week_start,
    weekly_active_addresses,
    month_start,
    monthly_active_addresses,
    txn_size_0_100,
    txn_size_100_1k,
    txn_size_1k_10k,
    txn_size_10k_100k,
    txn_size_100k_plus
from {{ ref('fact_stargate_metrics') }}