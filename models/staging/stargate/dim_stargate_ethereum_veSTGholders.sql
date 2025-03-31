{{config(materialized="table", snowflake_warehouse='STARGATE')}}
{{
    stargate_veSTGholders(
        'ethereum'
        , '0xAf5191B0De278C7286d6C7CC6ab6BB8A73bA2Cd6'
    )
}}