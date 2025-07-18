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
            ('USDY', 'A1KLoBrKBde8Ty9qtNQUtq3C2ortoC3u7twggz7sEto6', 6, 'ondo-us-dollar-yield', 0),
            ('USD*', 'BenJy1n3WTx9mTjEvy63e8Q1j4RqUc6E4VBMz3ir4Wo6', 6, 'perena-usd', 0), -- coingecko id is a placeholder until updated
            ('AUSD', 'AUSD1jCcCyPLybk1YnvPWsHQSrZ46dxwoMniN4N2UEB9', 6, 'agora-dollar', 0),
            ('sUSD', 'susdabGDNbhrnCa6ncrYo81u4s9GM8ecK2UwMyZiq4X', 6, 'solayer-usd', 0),
            ('USDG', '2u1tszSeqZ3qBWF3uNGPFc8TzMk2tdiwknnRMWGWjGWH', 6, 'global-dollar', 0),
            ('BUIDL', 'GyWgeqpy5GueU2YbkE8xqUeVEokCMMCEeUrfbtMw6phr', 6, 'blackrock-usd-institutional-digital-liquidity-fund', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
