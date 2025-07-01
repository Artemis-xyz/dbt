{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('USDC', '0x3894085Ef7Ff0f0aeDf52E2A2704928d1Ec074F1', 6, 'ibc-bridged-usdc', 0),
            ('USDT', '0xB75D0B03c06A926e488e2659DF1A861F860bD3d1', 6, 'ibc-bridged-usdt'),
            ('USDa', '0xff12470a969Dd362EB6595FFB44C82c959Fe9ACc', 18, 'usda-2', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)