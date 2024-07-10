{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="dim_avalanche_ccip_token_meta",
    )
}}

SELECT
   'avalanche' AS chain,
   token_contract,
   token_symbol
FROM (VALUES
    ('0x5947BB275c521040051D82396192181b413227A3', 'LINK'),
    ('0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7', 'WAVAX')
) a (token_contract, token_symbol)