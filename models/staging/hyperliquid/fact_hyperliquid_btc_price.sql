{{ config(materialized="table") }}

{{ hyperliquid_token_price("btc") }}