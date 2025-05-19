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
        chain="avalanche"
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
        on lower(t1.contract_address) = lower(treasury_data.contract_address) and t1.chain = 'avalanche'
)

, trader_joe_pool as (
    {{forward_filled_balance_for_address(
        chain="avalanche",
        address="0x330f77bda60d8dab14d2bb4f6248251443722009"
    )}}
)

,   trader_joe_balance as (
    select 
        date
        , 'trader_joe' as protocol
        , trader_joe_pool.contract_address
        , t1.symbol
        , balance_native
        , balance
    from trader_joe_pool
    inner join {{ ref("dim_coingecko_token_map") }} t1
        on lower(t1.contract_address) = lower(trader_joe_pool.contract_address) and t1.chain = 'avalanche'
    where trader_joe_pool.contract_address in ('0x2f6f07cdcf3588944bf4c42ac74ff24bf56e7590', '0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e')
)

, balances as (
    select * from treasury_balances
    union all
    select * from trader_joe_balance
)

select 
    date
    , protocol
    , 'avalanche' as chain
    , contract_address
    , symbol
    , balance_native
    , balance
from balances