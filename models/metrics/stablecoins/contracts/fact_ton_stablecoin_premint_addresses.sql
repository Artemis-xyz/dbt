{{ config(materialized="table") }}
-- Premint address can be an account owner or a token account
select contract_address, premint_address
from
    (
        values
            -- USDT
            (
                '0:B113A994B5024A16719F69139328EB759596C38A25F59028B146FECDC3621DFE',
                '0:23FA979918F1FE702DB9100BF843E87C7015ECCD39A4721E9B6BAC170BC04CE3'
            )
    ) as results(contract_address, premint_address)
