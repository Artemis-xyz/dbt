{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('USDC', '0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359', 6, 'usd-coin', 0),
            ('USDC', '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174', 6, 'usd-coin', 0),
            ('DAI', '0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063', 18, 'dai', 0),
            ('USDT', '0xc2132D05D31c914a87C6611C10748AEb04B58e8F', 6, 'tether', 0),
            ('TUSD', '0x2e1AD108fF1D8C782fcBbB89AAd783aC49586756', 18, 'true-usd', 0),
            ('FRAX', '0x45c32fa6df82ead1e2ef74d17b76547eddfaff89', 18, 'frax', 0),
            ('AUSD', '0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a', 6, 'agora-dollar', 0),
            ('DOLA', '0xbC2b48BC930Ddc4E5cFb2e87a45c379Aab3aac5C', 18, 'dola-usd', 0),
            ('crvUSD', '0xc4ce1d6f5d98d65ee25cf85e9f2e9dcfee6cb5d6', 18, 'crvusd', 0),
            ('MIM', '0x49a0400587a7f65072c87c4910449fdcc5c47242', 18, 'magic-internet-money-polygon', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
