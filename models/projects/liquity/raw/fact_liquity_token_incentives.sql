{{
    config(
        materialized='table',
        snowflake_warehouse='LIQUITY',
        database='LIQUITY',
        schema='raw',
        alias='fact_liquity_v1_token_incentives'
    )
}}

select
    block_timestamp::date as date,
    'ethereum' as chain,
    symbol as token,
    sum(amount) as token_incentives_native,
    sum(amount_usd) as token_incentives
from
    {{ source('ETHEREUM_FLIPSIDE', 'ez_token_transfers') }}
where
    1 = 1
    AND contract_address = lower('0x6dea81c8171d0ba574754ef6f8b412f2ed88c54d')
    AND from_address in (
        lower('0xd37a77E71ddF3373a79BE2eBB76B6c4808bDF0d5'),
        lower('0xD8c9D9071123a059C6E0A945cF0e0c82b508d816')
    )
    -- AND amount_usd > 5e6
GROUP BY 1,2,3