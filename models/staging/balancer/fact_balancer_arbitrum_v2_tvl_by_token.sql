{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
    )
}}

with arbitrum_v2_tvl_by_token as (
    {{ get_single_address_historical_balance_by_token_and_chain('arbitrum', '0xBA12222222228d8Ba445958a75a0704d566BF2C8', '2021-04-20') }}
)

select
    date,
    chain,
    'v2' as version,
    contract_address,
    symbol,
    balance_native,
    balance_usd
from arbitrum_v2_tvl_by_token