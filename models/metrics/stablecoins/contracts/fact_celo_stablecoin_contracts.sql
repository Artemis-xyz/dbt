{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('USDC', '0xcebA9300f2b948710d2653dD7B07f33A8B32118C', 6, 'usd-coin', 0),
            ('USDT', '0x48065fbBE25f71C9282ddf5e1cD6D6A887483D5e', 6, 'tether', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
