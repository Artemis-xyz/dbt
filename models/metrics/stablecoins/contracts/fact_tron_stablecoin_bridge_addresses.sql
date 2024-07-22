{{ config(materialized="table") }}
-- Premint addresses for ethereum are representative of USDT that has been bridged to other L2s
select contract_address, premint_address
from
    (
        values
            -- USDT
            -- BSC
            --      tron Bridge
            (
                'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t',
                'TT1DyeqXaaJkt6UhVYFWUXBXknaXnBudTK'
            )
    ) as results(contract_address, premint_address)
