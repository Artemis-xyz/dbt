{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('USDC', '0xaf88d065e77c8cc2239327c5edb3a432268e5831', 6, 'usd-coin', 0),
            ('DAI', '0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1', 18, 'dai', 0),
            (
                'USDC',
                '0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8',
                6,
                'usd-coin-ethereum-bridged',
                0
            ),
            ('USDT', '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9', 6, 'tether', 0),
            ('USDY', '0x35e050d3C0eC2d29D269a8EcEa763a183bDF9A9D', 18, 'ondo-us-dollar-yield', 0),
            ('USD0', '0x35f1c5cb7fb977e669fd244c567da99d8a3a6850', 18, 'usual-usd', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
