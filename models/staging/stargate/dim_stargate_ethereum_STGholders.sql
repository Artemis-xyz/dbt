{{config(materialized="incremental", snowflake_warehouse='STARGATE')}}
{{
    stargate_stg_holders(
        'ethereum'
        , '0xAf5191B0De278C7286d6C7CC6ab6BB8A73bA2Cd6'
        , '0x0e42acbd23faee03249daff896b78d7e79fbd58e'
    )
}}
