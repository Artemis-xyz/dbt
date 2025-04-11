{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="dim_bsc_ccip_token_meta",
    )
}}

SELECT
   'bsc' AS chain,
   token_contract,
   token_symbol
FROM (VALUES
    ('0x404460C6A5EdE2D891e8297795264fDe62ADBB75', 'LINK'),
    ('0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c', 'WBNB')
) a (token_contract, token_symbol)