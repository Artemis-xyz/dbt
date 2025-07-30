{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('USDC', '0x0b2c639c533813f4aa9d7837caf62653d097ff85', 6, 'usd-coin', 0),
            ('DAI', '0xda10009cbd5d07dd0cecc66161fc93d7c9000da1', 18, 'dai', 32079468),
            (
                'USDC',
                '0x7f5c764cbc14f9669b88837ca1490cca17c31607',
                6,
                'bridged-usd-coin-optimism',
                13913880
            ),
            ('USDT', '0x94b008aA00579c1307B0EF2c499aD98a8ce58e58', 6, 'tether', 8064094),
            ('FRAX', '0x2e3d870790dc77a83dd1d18184acc7439a53f475', 18, 'frax', 0),
            ('DOLA', '0x8ae125e8653821e851f12a49f7765db9a9ce7384', 18, 'dola-usd', 0),
            ('crvUSD', '0xc52d7f23a2e460248db6ee192cb23dd12bddcbf6', 18, 'crvusd', 0),
            ('S_USD', '0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9', 18, 'susd-optimism', 0),
            ('BUIDL', '0xa1CDAb15bBA75a80dF4089CaFbA013e376957cF5', 6, 'blackrock-usd-institutional-digital-liquidity-fund', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
