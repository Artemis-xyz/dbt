{{ config(materialized="table") }}
-- Premint address can be an account owner or a token account
select contract_address, premint_address
from
    (
        values
            -- AUSD
            (
                '0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a',
                '0xA02318f858128c8D2048eF47171249E9B4a0DedA'
            )
    ) as results(contract_address, premint_address)
