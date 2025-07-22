{{ config(materialized="table") }}
-- Premint addresses for ethereum are representative of USDT that has been bridged to other L2s
select contract_address, premint_address
from
    (
        values
            -- Un-released Supply 
            (
                '0xd077a400968890eacc75cdc901f0356c943e4fdb',
                '0x5754284f345afc66a98fbb0a0afe71e0f007b949'
            )
    ) as results(contract_address, premint_address)