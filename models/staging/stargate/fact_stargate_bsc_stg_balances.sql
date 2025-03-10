{{ config(materialized="table", snowflake_warehouse="STARGATE_MD") }}


{{forward_filled_token_balances(
    'bsc',
    '0xB0D502E938ed5f4df2E681fE6E419ff29631d62b'
)}}