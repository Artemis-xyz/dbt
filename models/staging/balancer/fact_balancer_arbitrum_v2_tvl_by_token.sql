{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
    )
}}

with arbitrum_v2_tvl_by_token as (
    {{ get_treasury_balance('arbitrum', '0xBA12222222228d8Ba445958a75a0704d566BF2C8', '2021-04-20') }}
)

select
    date,
    chain,
    'v2' as version,
    contract_address,
    token,
    native_balance,
    usd_balance
from arbitrum_v2_tvl_by_token