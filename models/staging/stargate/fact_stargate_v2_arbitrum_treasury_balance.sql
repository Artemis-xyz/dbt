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
        chain="arbitrum"
    )}}
)

, treasury_balances as (
    select
        date
        , case 
            when substr(symbol, 0, 2) = 'S*' then 'stargate'
            else 'wallet'
        end as protocol        
        , treasury_data.contract_address
        , upper(replace(symbol, 'S*', '')) as symbol
        , balance_native
        , balance
    from treasury_data
    inner join {{ ref("dim_coingecko_token_map") }} t1
        on lower(t1.contract_address) = lower(treasury_data.contract_address) and t1.chain = 'arbitrum'
)

select 
    date
    , protocol
    , 'arbitrum' as chain
    , contract_address
    , symbol
    , balance_native
    , balance
from treasury_balances