{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="dim_arbitrum_ccip_token_meta",
    )
}}

SELECT
   'arbitrum' AS blockchain
   , token_contract
   , token_symbol
FROM (VALUES
    ('0xf97f4df75117a78c1A5a0DBb814Af92458539FB4', 'LINK'),
    ('0x82aF49447D8a07e3bd95BD0d56f35241523fBab1', 'WETH')
) a (token_contract, token_symbol)