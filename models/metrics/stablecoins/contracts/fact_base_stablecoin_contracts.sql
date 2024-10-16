{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('USDC', '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913', 6, 'usd-coin', 0),
            ('EURC', '0x60a3E35Cc302bFA44Cb288Bc5a4F316Fdb1adb42', 6, 'euro-coin', 0),
            ('DAI', '0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb', 18, 'dai', 0),
            (
                'USDC',
                '0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA',
                6,
                'bridged-usd-coin-base',
                0
            )
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
