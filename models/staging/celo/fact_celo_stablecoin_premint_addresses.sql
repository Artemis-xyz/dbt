{{ config(materialized="table") }}
-- Premint addresses for ethereum are representative of USDT that has been bridged to other L2s
select contract_address, premint_address
from
    (
        values
            -- Un-released Supply 
            (
                '0x48065fbBE25f71C9282ddf5e1cD6D6A887483D5e',
                '0x5754284f345afc66a98fbb0a0afe71e0f007b949'
            )
    ) as results(contract_address, premint_address)