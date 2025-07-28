{{ config(materialized="table") }}
-- Premint addresses for ethereum are representative of USDT that has been bridged to other L2s
select contract_address, premint_address
from
    (
        values
            -- TETHER
            (
                'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t',
                'TKHuVq1oKVruCGLvqVexFs6dawKv6fQgFs'
            )
    ) as results(contract_address, premint_address)
