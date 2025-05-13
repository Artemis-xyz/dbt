{{ 
    config(
        materialized = 'table',
        snowflake_warehouse = 'BELIEVE',
        unique_key = 'block_timestamp'
    ) 
}}

with 
    launchpool_coins_minted as (
        select
            block_timestamp
            , signers[1]::string as coins_minted_address
        from solana_flipside.core.ez_events_decoded e,
            lateral flatten(input => e.decoded_instruction:accounts) as account
        where 
            e.program_id = 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
            and account.value:pubkey = 'FhVo3mqL8PW5pH5U2CN4XE33DokiyZnUwuGpH2hmHLuM'
            and signers[0] = '5qWya6UjwWnGVhdSBL3hyZ7B45jbk6Byt1hwd7ohEGXE'
    )

select
    block_timestamp
    , coins_minted_address
from launchpool_coins_minted
