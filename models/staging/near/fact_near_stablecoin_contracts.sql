{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('USDC', '17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1', 6, 'usd-coin', 0),
            ('USDT', 'usdt.tether-token.near', 6, 'tether', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)