{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('USDC', 'TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8', 6, 'usd-coin', 0),
            ('USDT', 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t', 6, 'tether', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
