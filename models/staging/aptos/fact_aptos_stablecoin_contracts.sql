{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('USDC', '0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b', 6, 'usd-coin', 0)
            , ('USDT', '0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b', 6, 'tether', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
