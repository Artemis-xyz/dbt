{{ config(materialized="table", snowflake_warehouse="SYNTHETIX") }}

select
    date(block_timestamp) as date,
    symbol as token, 
    'Ethereum' as chain,
    sum(coalesce(amount_usd, 0)) as fee_allocation
from ethereum_flipside.core.ez_token_transfers
where lower(to_address) in (lower('0xEb3107117FEAd7de89Cd14D463D340A2E6917769'), lower('0x49BE88F0fcC3A8393a59d3688480d7D253C37D2A'))
group by date, token
order by date desc, token