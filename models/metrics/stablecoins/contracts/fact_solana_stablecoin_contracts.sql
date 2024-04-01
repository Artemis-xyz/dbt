{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            (
                'USDC',
                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                6,
                'usd-coin',
                113000000
            ),
            ('USDT', 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', 6, 'tether', 0),
            ('EURC', 'HzwqbKZw8HxMN6bF2yFZNrht3c2iXXzpKcFu7uBEDKtr', 6, 'euro-coin', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
