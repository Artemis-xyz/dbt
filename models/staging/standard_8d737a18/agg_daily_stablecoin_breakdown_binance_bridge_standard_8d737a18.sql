{{ config(materialized="incremental", unique_id=['date', 'chain_id' 'address'], snowflake_warehouse="ANALYTICS_XL") }}
with 
    ethereum_data as (
        select 
            date
            , 'ethereum' as chain_name
            , 'eip155:1' as chain_id
            , 'binance' as bridge
            , address
            , stablecoin_supply as bridged_supply
            , stablecoin_supply_native as bridged_supply_native
        from {{ref("fact_ethereum_stablecoin_balances")}}
        where lower(contract_address) = lower('0xdAC17F958D2ee523a2206206994597C13D831ec7')
            and lower(address) = lower('0x47ac0Fb4F2D84898e4D9E7b4DaB3C24507a6D503')
            {% if is_incremental() %}
                and date >= (select DATEADD('day', -3, max(date)) from {{ this }})
            {% endif %}
    )
    , tron_data as (
        select 
            date
            , 'tron' as chain_name
            , 'bip122:00000000000000001ebf88508a03865c' as chain_id
            , 'binance' as bridge
            , address
            , stablecoin_supply as bridged_supply
            , stablecoin_supply_native as bridged_supply_native
        from {{ref("fact_tron_stablecoin_balances")}}
        where lower(contract_address) = lower('TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t')
            and lower(address) = lower('TT1DyeqXaaJkt6UhVYFWUXBXknaXnBudTK')
            {% if is_incremental() %}
                and date >= (select DATEADD('day', -3, max(date)) from {{ this }})
            {% endif %}
    )
select 
    date
    , chain_name
    , chain_id
    , bridge
    , address
    , bridged_supply
    , bridged_supply_native
from ethereum_data
union all
select 
    date
    , chain_name
    , chain_id
    , bridge
    , address
    , bridged_supply
    , bridged_supply_native
from tron_data