{{ config(materialized="table") }}
-- Premint address can be an account owner or a token account
select contract_address, premint_address
from
    (
        values
        -- AUSD Premint
            (
                '0x2053d08c1e2bd02791056171aab0fd12bd7cd7efad2ab8f6b9c8902f14df2ff2::ausd::AUSD',
                '0x0e9701736a20df4dce1d481f417b8666c8c9bbd9fd998085af99c624507a6528'
            ),
            (
                '0x2053d08c1e2bd02791056171aab0fd12bd7cd7efad2ab8f6b9c8902f14df2ff2::ausd::AUSD',
                '0x502504bb07e3e3ca030c2a1c025dda31307fb7ee912f5bf78cf8dbedfe948f67'
            )
    ) as results(contract_address, premint_address)
