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
    date,
    'solana' as chain,
    trading_volume,
    fees,
    revenue,
    total_supply_side_revenue,
    number_of_swaps,
    unique_traders,
    tvl
from {{ ref("ez_orca_metrics") }}