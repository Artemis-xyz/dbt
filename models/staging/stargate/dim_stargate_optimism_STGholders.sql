{{config(materialized="incremental", snowflake_warehouse='STARGATE')}}
{{
    stargate_stg_holders(
        'optimism'
        , '0x296F55F8Fb28E498B858d0BcDA06D955B2Cb3f97'
        , '0x43d2761ed16c89a2c4342e2b16a3c61ccf88f05b'
    )
}}
