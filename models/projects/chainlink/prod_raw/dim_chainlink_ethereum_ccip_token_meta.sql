{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="dim_ethereum_ccip_token_meta",
    )
}}

select
   'ethereum' as chain
   , token_contract
   , token_symbol
FROM (VALUES
    ('0x514910771AF9Ca656af840dff83E8264EcF986CA', 'LINK'),
    ('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', 'WETH')
) a (token_contract, token_symbol)