{{config(materialized="table", snowflake_warehouse='STARGATE')}}
{{
    stargate_veSTGholders(
        'arbitrum'
        , '0x6694340fc020c5E6B96567843da2df01b2CE1eb6'
    )
}}