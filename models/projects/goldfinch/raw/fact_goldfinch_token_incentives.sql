{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GOLDFINCH',
        database = 'goldfinch',
        schema = 'raw',
        alias = 'fact_goldfinch_token_incentives'
    )
}}

select 
    date(block_timestamp) as date,
    symbol as token,
    amount as amount_native,
    amount_usd,
    tx_hash
from 
ethereum_flipside.core.ez_token_transfers
where from_address in (
    lower('0x384860F14B39CcD9C89A73519c70cD5f5394D0a6'),
    lower('0x0Cd73c18C085dEB287257ED2307eC713e9Af3460'),
    lower('0xFD6FF39DA508d281C2d255e9bBBfAb34B6be60c3')
)
and contract_address = lower('0xdab396ccf3d84cf2d07c4454e10c8a6f5b008d2b')