{{
    config(
        materialized="table",
        snowflake_warehouse="SEI",
    )
}}
select contract_address
from
    (
        values
        -- use original contract addresses
        -- IBC original
            ('ibc/CA6FBFAF399474A06263E10D0CE5AEBBE15189D6D4B2DD9ADE61007E68EB9DB0'),
        -- EVM original
            ('0x9151434b16b9763660705744891fA906F660EcC5'),
            ('0xff12470a969Dd362EB6595FFB44C82c959Fe9ACc')
    ) as results(contract_address)