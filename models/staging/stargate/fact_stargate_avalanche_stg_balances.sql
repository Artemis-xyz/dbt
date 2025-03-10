{{ config(materialized="table", snowflake_warehouse="STARGATE_MD") }}


{{forward_filled_token_balances(
    'avalanche',
    '0x2f6f07cdcf3588944bf4c42ac74ff24bf56e7590'
)}}