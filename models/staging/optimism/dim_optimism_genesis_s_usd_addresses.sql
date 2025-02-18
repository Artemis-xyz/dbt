{{ config(materialized="table") }}
--This is used to get the addresses of the genesis SUSD contract
--Recursivly build this table for susd until we have all the addresses
--This table is used in the extract step raw_optimism_genesis_s_usd_stablecoin_balances
select
    distinct address
from {{ ref("fact_optimism_genesis_stablecoin_balances") }}
where lower(contract_address) = lower('0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9')
    and balance > 0
union 
select
    distinct address
from pc_dbt_db.prod.fact_optimism_address_balances_by_token
where lower(contract_address) = lower('0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9')
    and round(balance_token/1e18, 5) < 0