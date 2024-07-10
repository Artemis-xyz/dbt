{{ config(materialized="table") }}
SELECT
    symbol
    , contract_address
    , num_decimals
    , coingecko_id
    , initial_supply
FROM
    (
        VALUES
        ('USDC', 'TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8', 6, 'usd-coin', 0)
        , ('USDT', 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t', 6, 'tether', 0)
    )
        AS results (
            symbol, contract_address, num_decimals, coingecko_id, initial_supply
        )
