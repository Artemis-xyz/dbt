{{
    config(
        materialized="table",
        unique_key="unique_id",
        snowflake_warehouse="MEDIUM",
    )
}}

with
treasury_data as (
    {{ forward_filled_address_balances(
        artemis_application_id="stargate",
        type="treasury",
        chain="bsc"
    )}}
)

, treasury_balances as (
    select
        date
        , case 
            when substr(t1.symbol, 0, 2) = 'S*' then 'stargate'
            else 'wallet'
        end as protocol        
        , treasury_data.contract_address
        , upper(replace(t1.symbol, 'S*', '')) as symbol
        , balance_native
        , balance
    from treasury_data
    inner join {{ ref("dim_coingecko_token_map") }} t1
        on lower(t1.contract_address) = lower(treasury_data.contract_address) and t1.chain = 'bsc'
)

, pancakeswap_pool as (
    {{forward_filled_balance_for_address(
        chain="bsc",
        address="0x89a6be1ec107c911c3f2a1112f049f876ce033c9"
    )}}
)

, pancakeswap_balance as (
    select 
        date
        , 'pancakeswap' as protocol
        , pancakeswap_pool.contract_address
        , upper(case when t1.symbol = 'bsc-usd' then 'USDT' else t1.symbol end) as symbol
        , balance_native
        , balance
    from pancakeswap_pool
    inner join {{ ref("dim_coingecko_token_map") }} t1
        on lower(t1.contract_address) = lower(pancakeswap_pool.contract_address) and t1.chain = 'bsc'
)

, cake_price as (
    {{get_coingecko_price_with_latest('pancakeswap-token')}}
)

, staked_cake_date as (
    select 
        block_timestamp::date as date
        , 'pancakeswap' as protocol
        , '0x5692db8177a81a6c6afc8084c2976c9933ec1bab' as contract_address
        , 'veCAKE' as symbol
        , decoded_log:value::float/1e18 as balance_native
    from bsc_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0x5692db8177a81a6c6afc8084c2976c9933ec1bab')
        and lower(decoded_log:locker::string) = lower('0x6e690075eedBC52244Dd4822D9F7887d4f27442F')
        and event_name = 'Deposit'
)
, forward_fill_staked_cake as (
    select
        cake_price.date as date
        , protocol
        , contract_address
        , symbol
        , balance_native
        , balance_native * price as balance
    from staked_cake_date
    left join cake_price
    where cake_price.date > '2023-12-06'
)

, balances as (
    select * from pancakeswap_balance
    union all
    select * from treasury_balances
    union all
    select * from forward_fill_staked_cake
)

select 
    date
    , protocol
    , 'bsc' as chain
    , contract_address
    , symbol
    , balance_native
    , balance
from balances
