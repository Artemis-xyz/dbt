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
            ('EURC', 'HzwqbKZw8HxMN6bF2yFZNrht3c2iXXzpKcFu7uBEDKtr', 6, 'euro-coin', 0),
            ('PYUSD', '2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo', 6, 'paypal-usd', 0),
            ('USDS', 'USDSwr9ApdHk5bvJKMjzff41FfuX8bSxdKcR81vTwcA', 6, 'usds', 0),
            ('USDe', 'DEkqHyPN7GMRJ5cArtQFAWefqbZb33Hyf6s5iCwjEonT', 9, 'ethena-usde', 0),
            ('FDUSD', '9zNQRsGLjNKwCUU5Gq5LR8beUCPzQMVMqKAi3SSZh54u', 6, 'first-digital-usd', 0),
            ('USDY', 'A1KLoBrKBde8Ty9qtNQUtq3C2ortoC3u7twggz7sEto6', 6, 'ondo-us-dollar-yield', 0)
            -- ('USD*', '7MCXdudmztA6VwqsaWsNxFn8f7dVPxZorAHYbwwT2oTb', 9, 'perena-usd', 0) -- coingecko id is a placeholder until updated
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
