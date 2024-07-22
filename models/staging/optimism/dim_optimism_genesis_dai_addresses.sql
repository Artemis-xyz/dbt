{{ config(materialized="table") }}
--This is used to get the addresses of the genesis DAI contract
--Recursivly build this table for dai until we have all the addresses
--This table is used in the extract step raw_optimism_genesis_dai_stablecoin_balances
select
    distinct address
from {{ ref("fact_optimism_genesis_stablecoin_balances") }}
where lower(contract_address) = lower('0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1')
    and balance > 0
union 
select
    distinct address
from pc_dbt_db.prod.fact_optimism_stablecoin_balances
where lower(contract_address) = lower('0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1')
    and round(stablecoin_supply, 5) < 0