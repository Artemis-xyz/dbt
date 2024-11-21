{{
    config(
        materialized="table",
        snowflake_warehouse="RWA",
    )
}}

SELECT symbol, contract_address, num_decimals, coingecko_id, initial_supply FROM 
    (
        VALUES
            ('FOBXX', '0x60cfc2b186a4cf647486e42c42b11cc6d571d1e4', 18, 'franklin-onchain-u-s-government-money-fund', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
