{{
    config(
        materialized="table",
        snowflake_warehouse="RWA",
    )
}}

SELECT symbol, contract_address, num_decimals, coingecko_id, initial_supply FROM 
    (
        VALUES
            ('USDT', '0xda6087e69c51e7d31b6dbad276a3c44703dfdcad', 6, 'tether', 0)
            , ('USDC.e', '0x78add880a697070c1e765ac44d65323a0dcce913', 6, 'stargate-bridged-usdc-plume', 0)
            , ('pUSD', '0xdddd73f5df1f0dc31373357beac77545dc5a6f3f', 6, 'plume-usd', 0)
            , ('deUSD', '0x1271656F45e251f588847721BA2C561dd1F0223F', 18, 'elixir-deusd', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
