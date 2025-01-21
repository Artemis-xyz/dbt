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
            ('USD0', '0x73a15fed60bf67631dc6cd7bc5b6e8da8190acf5', 18, 'usual-usd', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
