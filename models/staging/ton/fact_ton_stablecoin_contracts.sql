{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply, premint_address
from
    (
        values
            (
                'USD₮',
                'EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs',
                6,
                'tether',
                0,
                'EQAj-peZGPH-cC25EAv4Q-h8cBXszTmkch6ba6wXC8BM4xdo'
            )
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply, premint_address)
