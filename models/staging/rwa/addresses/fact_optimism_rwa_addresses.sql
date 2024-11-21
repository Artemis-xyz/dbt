{{
    config(
        materialized="table",
        snowflake_warehouse="RWA",
    )
}}

SELECT symbol, contract_address, num_decimals, coingecko_id, initial_supply FROM 
    (
        VALUES
            ('BUIDL', '0xa1cdab15bba75a80df4089cafba013e376957cf5', 6, 'blackrock-usd-institutional-digital-liquidity-fund', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
