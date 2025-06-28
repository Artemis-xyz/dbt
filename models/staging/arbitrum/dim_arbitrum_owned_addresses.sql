{{
    config(
        materialized="table",
        snowflake_warehouse="ARBITRUM",
    )
}}

select address
from (values
    ('0xF3FC178157fb3c87548bAA86F9d24BA38E649B58'),  -- DAO Treasury
    ('0x2B9AcFd85440B7828DB8E54694Ee07b2B056B30C'),  -- Foundation Deployer
    ('0xD6c8a4E72584f24bd5517AfeD6c01D21477C17f6')   -- A Holding Address for ARB that's vested from the Vesting Address
) as v(address)