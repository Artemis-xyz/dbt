{{config(materialized="incremental", snowflake_warehouse='STARGATE')}}
{{
    stargate_stg_holders(
        'avalanche'
        , '0x2f6f07cdcf3588944bf4c42ac74ff24bf56e7590'
        , '0xca0f57d295bbce554da2c07b005b7d6565a58fce'
    )
}}
