{{ config(materialized="table", snowflake_warehouse="SYNTHETIX") }}

with fee_address_outflow_to_null_address as (
    select distinct tx_hash 
    from ethereum_flipside.core.ez_token_transfers
    where lower(from_address) = lower('0xfeefeefeefeefeefeefeefeefeefeefeefeefeef') 
        and lower(to_address) = lower('0x0000000000000000000000000000000000000000')
)

select
    date(block_timestamp) as date, 
    'Ethereum' as chain,
    symbol, 
    sum(coalesce(amount_usd, 0)) as treasury_cashflow,
from ethereum_flipside.core.ez_token_transfers
where lower(from_address) = lower('0x0000000000000000000000000000000000000000')
    and lower(to_address) in (lower('0xd939611c3ca425b4f6d4a82591eab3da43c2f4a0'), 
                                lower('0x99F4176EE457afedFfCB1839c7aB7A030a5e4A92'))
    and tx_hash in (select tx_hash from fee_address_outflow_to_null_address)
    and symbol is not null
group by 1, 2, 3





