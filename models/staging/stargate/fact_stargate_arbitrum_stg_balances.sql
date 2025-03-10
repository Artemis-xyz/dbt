{{ config(materialized="table", snowflake_warehouse="STARGATE_MD") }}


{{forward_filled_token_balances(
    'arbitrum',
    '0x6694340fc020c5E6B96567843da2df01b2CE1eb6'
)}}