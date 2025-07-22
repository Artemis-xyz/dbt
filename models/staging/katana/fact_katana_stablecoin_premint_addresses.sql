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
            ),
            (
                '0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a',
                '0xC050B14EFea76d16f4A06A16B3772e087237BD0c'
            ),
            (
                '0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a',
                '0x7e9324083d984c019d4c503814a5ab76465411f6'
            )
    ) as results(contract_address, premint_address)
