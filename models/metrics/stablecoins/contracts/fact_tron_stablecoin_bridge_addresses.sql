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
            ),
            -- USDT 
            -- BSC
            --      tron Bridge
            (
                'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t',
                'TWd4WrZ9wn84f5x1hZhL4DHvk738ns5jwb'
            )
    ) as results(contract_address, premint_address)
