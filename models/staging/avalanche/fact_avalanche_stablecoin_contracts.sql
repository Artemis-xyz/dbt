{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('USDC', '0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E', 6, 'usd-coin', 0),
            ('DAI', '0xd586E7F844cEa2F87f50152665BCbc2C279D8d70', 18, 'dai', 0),
            (
                'USDC',
                '0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664',
                6,
                'usd-coin-avalanche-bridged-usdc-e',
                0
            ),
            ('USDT', '0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7', 6, 'tether', 0),
            ('USDT', '0xc7198437980c041c805A1EDcbA50c1Ce5db95118', 6, 'tether', 0),
            ('EURC', '0xc891eb4cbdeff6e073e859e987815ed1505c2acd', 6, 'euro-coin', 0),
            ('AUSD', '0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a', 6, 'agora-dollar', 0),
            ('TUSD', '0x1c20e891bab6b1727d14da358fae2984ed9b59eb', 18, 'true-usd', 0),
            ('FRAX', '0xd24c2ad096400b6fbcd2ad8b24e7acbc21a1da64', 18, 'frax', 0),
            ('MIM', '0x130966628846bfd36ff31a822705796e8cb8c18d', 18, 'magic-internet-money-avalanche', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
