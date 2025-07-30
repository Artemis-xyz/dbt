{{
    config(
        materialized="table",
        snowflake_warehouse="MANTLE",
    )
}}

select address
from (values
    ('0x94FEC56BBEcEaCC71c9e61623ACE9F8e1B1cf473'),  -- MTreasuryL2
    ('0x87C62C3F9BDFc09200bCF1cbb36F233A65CeF3e6'),  -- MTreasuryL2-RB
    ('0x992b65556d330219e7e75C43273535847fEee262'),  -- MTreasuryL2-LP
    ('0xcD9Dab9Fa5B55EE4569EdC402d3206123B1285F4')   -- MTreasuryL2-FF
) as v(address)