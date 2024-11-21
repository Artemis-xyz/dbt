SELECT symbol, contract_address, num_decimals, coingecko_id, initial_supply FROM 
    (
        VALUES
            ('BUIDL', '0x53fc82f14f009009b440a706e31c9021e1196a2f', 6, 'blackrock-usd-institutional-digital-liquidity-fund', 0),
            ('FOBXX', '0xe08b4c1005603427420e64252a8b120cace4d122', 18, 'franklin-onchain-u-s-government-money-fund', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
