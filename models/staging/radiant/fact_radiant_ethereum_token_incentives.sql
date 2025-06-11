{{config(materialized="table", snowflake_warehouse='RADIANT')}}

select
    cast(block_timestamp as date) as date
    , 'ethereum' as chain
    , sum(amount) as amount_native
    , sum(amount_usd) as amount_usd
from ethereum_flipside.core.ez_token_transfers
where contract_address = lower('0x137dDB47Ee24EaA998a535Ab00378d6BFa84F893')
    and from_address in (lower('0x14b0A611230Dc48E9cc048d3Ae5279847Bf30919'))
group by date, chain