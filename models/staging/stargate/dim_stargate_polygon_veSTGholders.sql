{{config(materialized="table", snowflake_warehouse='STARGATE')}}
{{
    stargate_veSTGholders(
        'polygon'
        , '0x2F6F07CDcf3588944Bf4C42aC74ff24bF56e7590'
    )
}}
