{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('USDC', '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', 6, 'usd-coin', 0),
            ('DAI', '0x6B175474E89094C44Da98b954EedeAC495271d0F', 18, 'dai', 0),
            ('USDT', '0xdAC17F958D2ee523a2206206994597C13D831ec7', 6, 'tether', 0),
            (
                'BUSD',
                '0x4Fabb145d64652a948d72533023f6E7A623C7C53',
                18,
                'binance-usd',
                0
            ),
            (
                'USDe',
                '0x4c9edd5852cd905f086c759e8383e09bff1e68b3',
                18,
                'ethena-usde',
                0
            ),
            ('PYUSD', '0x6c3ea9036406852006290770BEdFcAbA0e23A0e8', 6, 'paypal-usd', 0),
            ('EURC', '0x1aBaEA1f7C830bD89Acc67eC4af516284b1bC33c', 6, 'euro-coin', 0),
            ('USDP', '0x8e870d67f660d95d5be530380d0ec0bd388289e1', 18, 'paxos-standard', 0),
            ('AUSD', '0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a', 6, 'agora-dollar', 0),
            ('USDS', '0xdC035D45d973E3EC169d2276DDab16f1e407384F', 18, 'usds', 0),
            ('FDUSD', '0xc5f0f7b66764F6ec8C8Dff7BA683102295E16409', 18, 'first-digital-usd', 0),
            ('USDY', '0x96F6eF951840721AdBF46Ac996b59E0235CB985C', 18, 'ondo-us-dollar-yield', 0),
            ('USD0', '0x73a15fed60bf67631dc6cd7bc5b6e8da8190acf5', 18, 'usual-usd', 0),
            ('USDX', '0xf3527ef8dE265eAa3716FB312c12847bFBA66Cef', 18, 'usdx-money-usdx', 0),
            ('USR', '0x66a1E37c9b0eAddca17d3662D6c05F4DECf3e110', 18, 'resolv-usr', 0),
            ('TUSD', '0x0000000000085d4780B73119b644AE5ecd22b376', 18, 'true-usd', 0),
            ('FRAX', '0x853d955acef822db058eb8505911ed77f175b99e', 18, 'frax', 0),
            ('RLUSD', '0x8292bb45bf1ee4d140127049757c2e0ff06317ed', 18, 'ripple-usd', 0),
            ('USDa', '0x8a60e489004ca22d775c5f2c657598278d17d9c2', 18, 'usda-2', 0),
            ('DEUSD', '0x15700b564ca08d9439c58ca5053166e8317aa138', 18, 'elixir-deusd', 0),
            ('GHO', '0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f', 18, 'gho', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
