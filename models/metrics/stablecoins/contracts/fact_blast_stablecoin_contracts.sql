{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('USDB', '0x4300000000000000000000000000000000000003', 18, 'usdb', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
