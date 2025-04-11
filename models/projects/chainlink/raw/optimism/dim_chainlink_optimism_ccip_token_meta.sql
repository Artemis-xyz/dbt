{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="dim_optimism_ccip_token_meta",
    )
}}


SELECT
   'optimism' AS chain,
   token_contract,
   token_symbol
FROM (VALUES
    ('0x350a791Bfc2C21F9Ed5d10980Dad2e2638ffa7f6', 'LINK'),
    ('0x4200000000000000000000000000000000000006', 'WETH')
) a (token_contract, token_symbol)