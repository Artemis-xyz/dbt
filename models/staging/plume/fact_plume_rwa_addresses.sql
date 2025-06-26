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
            , ('nALPHA', '0x593ccca4c4bf58b7526a4c164ceef4003c6388db', 6, 'nest-alpha-vault', 0)
            , ('nCREDIT', '0xa5f78b2a0ab85429d2dfbf8b60abc70f4cec066c', 6, 'nest-credit-vault', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
