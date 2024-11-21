SELECT symbol, contract_address, num_decimals, coingecko_id, initial_supply FROM 
    (
        VALUES
            ('BUIDL', '0x7712c34205737192402172409a8F7ccef8aA2AEc', 6, 'blackrock-usd-institutional-digital-liquidity-fund', 0),
            ('OUSG', '0x1b19c19393e2d034d8ff31ff34c81252fcbbee92', 18, 'ousg', 0),
            ('USDY', '0x96f6ef951840721adbf46ac996b59e0235cb985c', 18, 'ondo-us-dollar-yield', 0),
            ('USYC', '0x136471a34f6ef19fe571effc1ca711fdb8e49f2b', 6, 'hashnote-usyc', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
