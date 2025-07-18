{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('RLUSD', 'rMxCKbEDwqr76QuheSUMdEGf4B9xJ8m5De', 0, 'ripple-usd', 0),
            ('USDC', 'rGm7WCVp9gb4jZHWTEtGUr4dd74z2XuWhE', 0, 'usd-coin', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
