{{ config(materialized="table") }}
-- Premint address can be an account owner or a token account
select contract_address, premint_address
from
    (
        values
            -- USDa
            (
                '0xff12470a969Dd362EB6595FFB44C82c959Fe9ACc',
                '0x1414E53BB41ad71aa866ea23d57f805E33316D38'
            )
    ) as results(contract_address, premint_address)
