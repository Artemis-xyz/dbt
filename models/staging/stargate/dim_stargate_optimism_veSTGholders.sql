{{config(materialized="table", snowflake_warehouse='STARGATE')}}
{{
    stargate_veSTGholders(
        'optimism'
        , '0x296F55F8Fb28E498B858d0BcDA06D955B2Cb3f97'
    )
}}
