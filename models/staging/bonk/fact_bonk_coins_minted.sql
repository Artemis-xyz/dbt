{{
    config(
        materialized='incremental',
        unique_key='mint_address',
        snowflake_warehouse='BONK',
    )
}}
with data as (
    select 
        date_trunc('day', min(block_timestamp)) as launch_date
        , min(block_timestamp) as block_timestamp
        , decoded_instruction:accounts[6]:"pubkey"::string as mint_address
        , min_by(decoded_instruction, block_timestamp) as decoded_instruction
    from solana_flipside.core.fact_decoded_instructions 
    where lower(program_id) = lower('LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj')
    {% if is_incremental() %}
        and block_timestamp > (select dateadd(day, -3, to_date(sysdate())) from {{ this }})
    {% else %}
        and block_timestamp >= '2025-04-20'
    {% endif %}
    and event_type = 'initialize'
    group by mint_address
)
select 
    launch_date
    , block_timestamp
    , decoded_instruction:accounts[6]:"pubkey"::string as mint_address
    , decoded_instruction:"args":"baseMintParam":"decimals"::int as decimals
    , decoded_instruction:"args":"baseMintParam":"name"::string as name
    , decoded_instruction:"args":"baseMintParam":"symbol"::string as symbol
    , decoded_instruction:"args":"curveParam":"constant":"data":"supply"::int as total_supply
from data
