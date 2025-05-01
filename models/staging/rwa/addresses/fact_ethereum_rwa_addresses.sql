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
            ('BUIDL', '0x6a9da2d710bb9b700acde7cb81f10f1ff8c89041', 6, 'blackrock-usd-institutional-digital-liquidity-fund', 0),
            ('OUSG', '0x1b19c19393e2d034d8ff31ff34c81252fcbbee92', 18, 'ousg', 0),
            ('USDY', '0x96f6ef951840721adbf46ac996b59e0235cb985c', 18, 'ondo-us-dollar-yield', 0),
            ('USDY', '0xe86845788d6e3e5c2393ade1a051ae617d974c09', 18, 'ondo-us-dollar-yield', 0),
            ('USYC', '0x136471a34f6ef19fe571effc1ca711fdb8e49f2b', 6, 'hashnote-usyc', 0),
            ('PAXG', '0x45804880de22913dafe09f4980848ece6ecbaf78', 18, 'pax-gold', 0),
            ('XAUT', '0x68749665ff8d2d112fa859aa293f07a622782f38', 6, 'tether-gold', 0),
            ('TBILL', '0xdd50c053c096cb04a3e3362e2b622529ec5f2e8a', 6, 'openeden-tbill', 0),
            ('FOBXX', '0x3ddc84940ab509c11b20b76b466933f40b750dc9', 18, 'franklin-onchain-u-s-government-money-fund', 0),
            ('WTGXX', '0x1fecf3d9d4fee7f2c02917a66028a48c6706c179', 18, 'wisdomtree-government-money-market-digital-fund', 0),
            ('USTB', '0x43415eb6ff9db7e26a15b704e7a3edce97d31c4e', 6, 'superstate-short-duration-us-government-securities-fund-ustb', 0),
            ('USTBL', '0xe4880249745eac5f1ed9d8f7df844792d560e750', 5, 'spiko-us-t-bills-money-market-fund', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
