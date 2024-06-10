{{ config(snowflake_warehouse="STADER", materialized="table") }}
-- depends_on: {{ source("PROD_LANDING", "raw_ethx_staked_count_ethereum") }}
with raw_eth_supply as (
    {{ raw_partitioned_array_to_fact_table_many_columns('landing_database.prod_landing.raw_ethx_staked_count_ethereum', 'date', ['totalAssets', 'getExchangeRate']) }}
),
eth_supply as (
    select 
        date,
        totalAssets * getExchangeRate as total_supply
    from raw_eth_supply
),
eth_supply_forward_filled as (
    {{ forward_fill('date', 'total_supply', 'eth_supply') }}
)
select
    eth_supply_forward_filled.date as date,
    coalesce(eth_supply_forward_filled.value, 0) as total_supply
from eth_supply_forward_filled