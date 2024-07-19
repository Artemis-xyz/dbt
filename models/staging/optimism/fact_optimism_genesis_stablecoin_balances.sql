{{ config(materialized="table") }}

{% set genesis_timestamp = "2021-11-10 00:00:00.000" %}

with
    dai as ({{genesis_stablecoin_balances("raw_optimism_genesis_dai_stablecoin_balances", genesis_timestamp)}})

    , usdce as ({{genesis_stablecoin_balances("raw_optimism_genesis_usdce_stablecoin_balances", genesis_timestamp)}})

    , usdt as ({{genesis_stablecoin_balances("raw_optimism_genesis_usdt_stablecoin_balances", genesis_timestamp)}})

select * from dai
union all
select * from usdce
union all 
select * from usdt