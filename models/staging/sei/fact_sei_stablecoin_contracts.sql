{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
        -- use the original contract addresses
        -- IBC original 
            ('USDC', 'ibc/CA6FBFAF399474A06263E10D0CE5AEBBE15189D6D4B2DD9ADE61007E68EB9DB0', 6, 'usd-coin', 0),
        -- EVM original
            ('USDT', '0x9151434b16b9763660705744891fA906F660EcC5', 6, 'tether', 0),
            ('USDa', '0xff12470a969Dd362EB6595FFB44C82c959Fe9ACc', 18, 'usda-2', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)