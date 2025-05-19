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
        chain="polygon"
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
        on lower(t1.contract_address) = lower(treasury_data.contract_address) and t1.chain = 'polygon'
)

, dex_pool as (
    {{forward_filled_balance_for_address(
        chain="polygon",
        address="0xa34ec05da1e4287fa351c74469189345990a3f0c"
    )}}
)

,   dex_balance as (
    select 
        date
        , 'sushiswap' as protocol
        , dex_pool.contract_address
        , t1.symbol
        , balance_native
        , balance
    from dex_pool
    inner join {{ ref("dim_coingecko_token_map") }} t1
        on lower(t1.contract_address) = lower(dex_pool.contract_address) and t1.chain = 'polygon'
    where dex_pool.contract_address in ('0x2791bca1f2de4661ed88a30c99a7a9449aa84174', '0x2f6f07cdcf3588944bf4c42ac74ff24bf56e7590')
)

, balances as (
    select * from treasury_balances
    union all
    select * from dex_balance
)

select 
    date
    , protocol
    , 'polygon' as chain
    , contract_address
    , symbol
    , balance_native
    , balance
from balances
