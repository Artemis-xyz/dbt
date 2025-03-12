{{ config(materialized="table", snowflake_warehouse="STARGATE_MD") }}


{{forward_filled_token_balances(
    'optimism',
    '0x296F55F8Fb28E498B858d0BcDA06D955B2Cb3f97'
)}}