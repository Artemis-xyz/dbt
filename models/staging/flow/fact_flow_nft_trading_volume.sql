{{ config(materialized="view", snowflake_warehouse="FLOW") }}

{{ nft_trading_volume("flow") }}
