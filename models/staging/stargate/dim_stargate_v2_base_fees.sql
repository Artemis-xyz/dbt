{{config(materialized="incremental", snowflake_warehouse='STARGATE')}}
{{
    stargate_fees(
        'base'
        , '0xB5320B0B3a13cC860893E2Bd79FCd7e13484Dda2'
    )
}}