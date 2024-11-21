{{
    config(
        materialized="table",
        snowflake_warehouse="RWA",
    )
}}

SELECT symbol, contract_address, num_decimals, coingecko_id, initial_supply FROM 
    (
        VALUES
            ('USDY', '0x5be26527e817998a7206475496fde1e68957c5a6', 18, 'ondo-us-dollar-yield', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
