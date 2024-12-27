{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
    )
}}

with ethereum_v2_tvl_by_token as (
    {{ get_treasury_balance('ethereum', '0xBA12222222228d8Ba445958a75a0704d566BF2C8', '2021-04-20',blacklist=('0x1a44e35d5451e0b78621a1b3e7a53dfaa306b1d0')) }}
)

select
    date,
    chain,
    'v2' as version,
    contract_address,
    token,
    native_balance,
    usd_balance
from ethereum_v2_tvl_by_token
where usd_balance < 1e9 -- filter out DQ issues, no tokens have a balance anywhere near 1bn