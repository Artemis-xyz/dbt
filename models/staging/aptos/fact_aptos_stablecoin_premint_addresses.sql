{{ config(materialized="table") }}
-- Premint addresses for ethereum are representative of USDT that has been bridged to other L2s
select contract_address, premint_address
from
    (
        values
            -- Un-released Supply 
            (
                '0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b',
                '0xd5b71ee4d1bad5cb7f14c880ee55633c7befcb7384cf070919ea5c481019a4e9'
            )
    ) as results(contract_address, premint_address)