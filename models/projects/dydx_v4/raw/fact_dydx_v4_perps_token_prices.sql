{{
    config(
        materialized = "table",
        snowflake_warehouse = "DYDX",
        database = "dydx_v4",
        schema = "raw",
        alias = "fact_perps_token_prices"
    )
}}

select
    block_timestamp
    , tx_hash
    , 'dydx_v4' as chain
    , 'dydx_v4' as app
    , symbol
    , price
    , null as token_address
from {{ ref('fact_dydx_v4_perps_prices') }}
