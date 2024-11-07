--depends_on: {{ source("PROD_LANDING", "raw_hyperliquid_eth_price_data") }}
{{ config(materialized="table") }}


{{ hyperliquid_token_price("eth") }}