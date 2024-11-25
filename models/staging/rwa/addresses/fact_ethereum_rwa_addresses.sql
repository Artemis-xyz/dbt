{{
    config(
        materialized="table",
        snowflake_warehouse="RWA",
    )
}}

SELECT symbol, contract_address, num_decimals, coingecko_id, initial_supply FROM 
    (
        VALUES
            ('BUIDL', '0x7712c34205737192402172409a8f7ccef8aa2aec', 6, 'blackrock-usd-institutional-digital-liquidity-fund', 0),
            ('OUSG', '0x1b19c19393e2d034d8ff31ff34c81252fcbbee92', 18, 'ousg', 0),
            ('USDY', '0x96f6ef951840721adbf46ac996b59e0235cb985c', 18, 'ondo-us-dollar-yield', 0),
            ('USYC', '0x136471a34f6ef19fe571effc1ca711fdb8e49f2b', 6, 'hashnote-usyc', 0),
            ('PAXG', '0x45804880de22913dafe09f4980848ece6ecbaf78', 18, 'pax-gold', 0),
            ('XAUT', '0x68749665ff8d2d112fa859aa293f07a622782f38', 6, 'tether-gold', 0),
            ('TBILL', '0xdd50c053c096cb04a3e3362e2b622529ec5f2e8a', 6, 'openeden-tbill', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
