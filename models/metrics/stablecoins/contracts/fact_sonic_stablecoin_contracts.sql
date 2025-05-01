{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('USDC', '0x29219dd400f2Bf60E5a23d13Be72B486D4038894', 6, 'usd-coin', 0)
            , ('USDT', '0x6047828dc181963ba44974801FF68e538dA5eaF9', 6, 'tether', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
