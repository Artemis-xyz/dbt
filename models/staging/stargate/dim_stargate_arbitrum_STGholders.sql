{{config(materialized="incremental", snowflake_warehouse='STARGATE')}}
{{
    stargate_stg_holders(
        'arbitrum'
        , '0x6694340fc020c5E6B96567843da2df01b2CE1eb6'
        , '0xfbd849e6007f9bc3cc2d6eb159c045b8dc660268'
    )
}}
