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
            ('USDX', '0xf3527ef8dE265eAa3716FB312c12847bFBA66Cef', 18, 'usdx-money-usdx', 0),
            ('TUSD', '0x40af3827F39D0EAcBF4A168f8D4ee67c121D11c9', 18, 'true-usd', 0),
            ('FRAX', '0x90c97f71e18723b0cf0dfa30ee176ab653e89f40', 18, 'frax', 0),
            ('USDa', '0x9356086146be5158e98ad827e21b5cf944699894', 18, 'usda-2', 0),
            ('DOLA', '0x2f29bc0ffaf9bff337b31cbe6cb5fb3bf12e5840', 18, 'dola-usd', 0),
            ('USDF', '0x5A110fC00474038f6c02E89C707D638602EA44B5', 18, 'astherus-usdf', 0),
            ('crvUSD', '0xe2fb3f127f5450dee44afe054385d74c392bdef4', 18, 'crvusd', 0),
            ('LISUSD', '0x0782b6d8c4551B9760e74c0545a9bCD90bdc41E5', 18, 'helio-protocol-hay', 0),
            ('USDD', '0x392004BEe213F1FF580C867359C246924f21E6Ad', 18, 'usdd', 0),
            ('AEUR', '0xA40640458FBc27b6EefEdeA1E9C9E17d4ceE7a21', 18, 'anchored-coins-eur', 0),
            ('IDRT', '0x66207E39bb77e6B99aaB56795C7c340C08520d83', 2, 'rupiah-token', 0),
            ('TRYB', '0xC1fdbed7Dac39caE2CcC0748f7a80dC446F6a594', 6, 'bilira', 0),
            ('IDRX', '0x649a2DA7B28E0D54c13D5eFf95d3A660652742cC', 0, 'idrx', 0),
            ('USD1', '0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d', 18, 'usd1-wlfi', 0),
            ('XUSD', '0xf81ac2e1a0373dde1bce01e2fe694a9b7e3bfcb9', 6, 'straitsx-xusd', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
