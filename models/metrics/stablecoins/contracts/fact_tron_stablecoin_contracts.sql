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
        , ('USDT', 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t', 6, 'tether', 0),
        ('TUSD', 'TUpMhErZL2fhh4sVNULAbNKLokS4GjC1F4', 18, 'true-usd', 0),
        ('USDD', 'TXDk8mbtRbXeYuMNS83CfKPaYYT8XWv9Hz', 18, 'usdd', 0),
        ('USD1', 'TPFqcBAaaUMCSVRCqPaQ9QnzKhmuoLR6Rc', 18, 'usd1-wlfi', 0)
    )
        AS results (
            symbol, contract_address, num_decimals, coingecko_id, initial_supply
        )
