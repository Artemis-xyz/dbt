{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('USDT', '0xB8CE59FC3717ada4C02eaDF9682A9e934F625ebb', 6, 'tether', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)