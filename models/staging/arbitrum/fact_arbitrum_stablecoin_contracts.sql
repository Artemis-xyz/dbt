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
            ('USD0', '0x35f1c5cb7fb977e669fd244c567da99d8a3a6850', 18, 'usual-usd', 0),
            ('USDX', '0xf3527ef8dE265eAa3716FB312c12847bFBA66Cef', 18, 'usdx-money-usdx', 0),
            ('TUSD', '0x4D15a3A2286D883AF0AA1B3f21367843FAc63E07', 18, 'true-usd', 0),
            ('FRAX', '0x17fc002b466eec40dae837fc4be5c67993ddbd6f', 18, 'frax', 0),
            ('GHO', '0x7dfF72693f6A4149b17e7C6314655f6A9F7c8B33', 18, 'gho', 0),
            ('DOLA', '0x6a7661795c374c0bfc635934efaddff3a7ee23b6', 18, 'dola-usd', 0),
            ('crvUSD', '0x498bf2b1e120fed3ad3d42ea2165e9b73f99c1e5', 18, 'crvusd', 0),
            ('USDD', '0x680447595e8b7b3Aa1B43beB9f6098C79ac2Ab3f', 18, 'usdd', 0),
            ('S_USD', '0xa970af1a584579b618be4d69ad6f73459d112f95', 18, 'nusd', 0),
            ('MIM', '0xfea7a6a0b346362bf88a9e4a88416b77a57d6c2a', 18, 'magic-internet-money-arbitrum', 0),
            ('BUIDL', '0xA6525Ae43eDCd03dC08E775774dCAbd3bb925872', 6, 'blackrock-usd-institutional-digital-liquidity-fund', 0),
            ('PYUSD', '0x46850aD61C2B7d64d08c9C754F45254596696984', 6, 'paypal-usd', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
