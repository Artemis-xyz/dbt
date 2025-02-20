{{config(materialized="incremental", snowflake_warehouse='STARGATE')}}
{{
    stargate_fees(
        'optimism'
        , '0x1322871e4ab09Bc7f5717189434f97bBD9546e95'
    )
}}