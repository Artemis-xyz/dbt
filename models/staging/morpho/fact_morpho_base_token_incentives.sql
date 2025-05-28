{{ config(materialized='table', snowflake_warehouse='MORPHO') }}

with 
    base_token_incentives as (
        select 
            block_timestamp
            , tx_hash
            , from_address
            , to_address
            , contract_address
            , amount as amount_native
            , amount_usd
        from {{ source('base_flipside', 'ez_token_transfers') }}
        where 1=1
            and lower(contract_address) = lower('0xBAa5CC21fd487B8Fcc2F632f3F4E8D37262a0842')
            and lower(from_address) = lower('0x5400dbb270c956e8985184335a1c62aca6ce1333')
    )

select
    date(block_timestamp) as date
    , 'base' as chain
    , sum(amount_native) as amount_native
    , sum(amount_usd) as amount_usd
from base_token_incentives
group by 1
