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
        chain="mantle"
    )}}
)

, treasury_balances as (
    select
        date
        , 'wallet' as protocol
        , treasury_data.contract_address
        , t1.symbol
        , balance_native
        , balance
    from treasury_data
    inner join {{ ref("dim_coingecko_token_map") }} t1
        on lower(t1.contract_address) = lower(treasury_data.contract_address) and t1.chain = 'mantle'
)

select 
    date
    , protocol
    , 'mantle' as chain
    , contract_address
    , symbol
    , balance_native
    , balance
from treasury_balances