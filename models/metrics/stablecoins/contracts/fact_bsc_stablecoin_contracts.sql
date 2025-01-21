{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('USDT', '0x55d398326f99059fF775485246999027B3197955', 18, 'tether', 0),
            ('USDC', '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d', 18, 'usd-coin', 0),
            ('DAI', '0x1AF3F329e8BE154074D8769D1FFa4eE058B1DBc3', 18, 'dai', 0),
            ('BUSD', '0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56', 18, 'binance-usd', 0),
            ('FDUSD', '0xc5f0f7b66764F6ec8C8Dff7BA683102295E16409', 18, 'first-digital-usd', 0),
            ('USDX', '0xf3527ef8dE265eAa3716FB312c12847bFBA66Cef', 18, 'usdx-money-usdx', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
