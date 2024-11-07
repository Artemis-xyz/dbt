{{
    config(
        materialized="table",
        snowflake_warehouse="HYPERLIQUID",
        database="hyperliquid",
        schema="raw",
        alias="fact_perp_token_prices",
    )
}}
with
    perp_prices as (
       {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_hyperliquid_btc_price"),
                    ref("fact_hyperliquid_eth_price"),
                ],
            )
        }}
    )
 select
    timestamp
    , 'DeFi' as category
    , 'hyperliquid' as app
    , price
    , size
    , type
    , symbol
from perp_prices