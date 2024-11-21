{{
    config(
        materialized="table",
        snowflake_warehouse="RWA",
    )
}}

SELECT symbol, contract_address, num_decimals, coingecko_id, initial_supply FROM 
    (
        VALUES
            ('BUIDL', '0xa6525ae43edcd03dc08e775774dcabd3bb925872', 6, 'blackrock-usd-institutional-digital-liquidity-fund', 0),
            ('TBILL', '0xf84d28a8d28292842dd73d1c5f99476a80b6666a', 6, 'openeden-tbill', 0),
            ('USDY', '0x35e050d3c0ec2d29d269a8ecea763a183bdf9a9d', 18, 'ondo-us-dollar-yield', 0),
            ('FOBXX', '0xb9e4765bce2609bc1949592059b17ea72fee6c6a', 18, 'franklin-onchain-u-s-government-money-fund', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
