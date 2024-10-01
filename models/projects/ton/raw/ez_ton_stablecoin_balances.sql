{{
    config(
        materialized="view",
        snowflake_warehouse="TON",
        database="ton",
        schema="raw",
        alias="ez_stablecoin_balances",
    )
}}

select
    date
    , chain
    , address
    , contract_address
    , symbol
    , stablecoin_supply_native
    , stablecoin_supply
    , unique_id
from {{ ref("fact_ton_stablecoin_balances") }}