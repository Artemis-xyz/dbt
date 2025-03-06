{{config(materialized="table", snowflake_warehouse='MEDIUM')}}
{{
    stargate_veSTGholders(
        'polygon'
        , '0x2F6F07CDcf3588944Bf4C42aC74ff24bF56e7590'
        , '0x3ab2da31bbd886a7edf68a6b60d3cde657d3a15d'
    )
}}
