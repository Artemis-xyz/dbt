{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="dim_base_ccip_token_meta",
    )
}}

SELECT
   'base' AS chain,
   token_contract,
   token_symbol
FROM (VALUES
    ('0x88Fb150BDc53A65fe94Dea0c9BA0a6dAf8C6e196', 'LINK'),
    ('0x4200000000000000000000000000000000000006', 'WETH')
) a (token_contract, token_symbol)