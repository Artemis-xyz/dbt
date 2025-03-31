{{config(materialized="table", snowflake_warehouse='STARGATE')}}
{{
    stargate_veSTGholders(
        'avalanche'
        , '0x2f6f07cdcf3588944bf4c42ac74ff24bf56e7590'
    )
}}
