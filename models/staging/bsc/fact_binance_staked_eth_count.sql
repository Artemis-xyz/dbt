{{ config(materialized="table") }}
-- depends_on: {{ source("PROD_LANDING", "raw_beth_staked_count_ethereum") }}
-- depends_on: {{ source("PROD_LANDING", "raw_beth_staked_count_bsc") }}
with raw_eth_supply as (
    {{ raw_partitioned_array_to_fact_table_many_columns('landing_database.prod_landing.raw_beth_staked_count_ethereum', 'date', ['totalSupply', 'exchangeRate']) }}
),
eth_supply as (
    select 
        date,
        totalSupply * exchangeRate as total_supply
    from raw_eth_supply
),
eth_supply_forward_filled as (
    {{ forward_fill('date', 'total_supply', 'eth_supply') }}
),
raw_bsc_supply as (
    {{ raw_partitioned_array_to_fact_table_many_columns('landing_database.prod_landing.raw_beth_staked_count_bsc', 'date', ['totalSupply', 'exchangeRate']) }}
),
bsc_supply as (
    select 
        date,
        totalSupply * exchangeRate as total_supply
    from raw_bsc_supply
),
bsc_supply_forward_filled as (
    {{ forward_fill('date', 'total_supply', 'bsc_supply') }}
)
select
    coalesce(eth_supply_forward_filled.date, bsc_supply_forward_filled.date) as date,
    coalesce(eth_supply_forward_filled.value, 0) + 
    coalesce(bsc_supply_forward_filled.value, 0) 
        as total_supply
from eth_supply_forward_filled
full join bsc_supply_forward_filled 
    on 
eth_supply_forward_filled.date = bsc_supply_forward_filled.date
