{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('RLUSD', 'rMxCKbEDwqr76QuheSUMdEGf4B9xJ8m5De', 0, 'ripple-usd', 0),
            ('USDC', 'rcEGREd8NmkKRE8GE424sksyt1tJVFZwu', 0, 'usd-coin', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
