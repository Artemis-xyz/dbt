{{ config(materialized="table") }}
-- Premint addresses for ethereum are representative of USDT that has been bridged to other L2s
select contract_address, premint_address
from
    (
        values
            --USDT Treasury
            (
                '0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7',
                '0x5754284f345afc66a98fbb0a0afe71e0f007b949'
            )
    ) as results(contract_address, premint_address)
