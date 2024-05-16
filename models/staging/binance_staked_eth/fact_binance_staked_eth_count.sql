{{ config(materialized="table") }}

with eth_supply as (
    {{ daily_erc20_total_supply("0xa2E3356610840701BDf5611a53974510Ae27E2e1", 18, "ethereum") }}
),
eth_supply_forward_filled as (
    {{ forward_fill('date', 'total_supply', 'eth_supply') }}
),
bsc_supply as (
    ({{ daily_erc20_total_supply("0xa2E3356610840701BDf5611a53974510Ae27E2e1", 18,"bsc") }})
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