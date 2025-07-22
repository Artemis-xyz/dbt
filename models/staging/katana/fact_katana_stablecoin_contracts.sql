{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('AUSD', '0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a', 6, 'agora-dollar', 0)
            , ('USDC', '0x203A662b0BD271A6ed5a60EdFbd04bFce608FD36', 6, 'usd-coin', 0)
            , ('USDT', '0x2dca96907fde857dd3d816880a0df407eeb2d2f2', 6, 'tether', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
