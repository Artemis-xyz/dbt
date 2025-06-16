{{
    config(
        materialized="table",
        snowflake_warehouse="RWA",
    )
}}

SELECT symbol, contract_address, num_decimals, coingecko_id, initial_supply FROM 
    (
        VALUES
            ('nETF', '0xdea736937d464d288ec80138bcd1a2e109a200e3', 6, 'nest-etf-vault', 0)
            , ('nELIXIR', '0x9fbc367b9bb966a2a537989817a088afcaffdc4c', 6, 'nest-institutional-core', 0)
            , ('nINSTO', '0xbfc5770631641719cd1cf809d8325b146aed19de', 6, 'nest-institutional-vault', 0)
            , ('nPAYFI', '0xb52b090837a035f93a84487e5a7d3719c32aa8a9', 6, 'nest-payfi-vault', 0)
            , ('nTBILL', '0xe72fe64840f4ef80e3ec73a1c749491b5c938cb9', 6, 'nest-treasury-vault', 0)
            , ('nBasis', '0x11113ff3a60c2450f4b22515cb760417259ee94b', 6, 'nest-basis-vault', 0)
            , ('USTB', '0xe4fa682f94610ccd170680cc3b045d77d9e528a8', 6, 'superstate-short-duration-us-government-securities-fund-ustb', 0)
            , ('USCC', '0x4c21b7577c8fe8b0b0669165ee7c8f67fa1454cf', 6, 'superstate-uscc', 0)
            , ('USDC.e', '0x78add880a697070c1e765ac44d65323a0dcce913', 6, 'stargate-bridged-usdc-plume', 0)
            , ('pUSD', '0xdddd73f5df1f0dc31373357beac77545dc5a6f3f', 6, 'plume-usd', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
