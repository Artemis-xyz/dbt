{{
    config(
        materialized="table",
        snowflake_warehouse="OPTIMISM",
        database="optimism",
        schema="raw",
        alias="dim_optimism_owned_addresses",
    )
}}

select address
-- Source: https://community.optimism.io/welcome/faq/dashboard-trackers
from (values
    ('0x2A82Ae142b2e62Cb7D10b55E323ACB1Cab663a26'), -- OP Treasury Address for Foundation Allocated Budget
    ('0x2501c477D0A35545a387Aa4A3EEe4292A9a8B3F0'), -- OP Treasury Address for Foundation Approved Budget
    ('0x19793c7824Be70ec58BB673CA42D2779d12581BE'), -- OP Foundation Grants Wallet
    ('0xE4553b743E74dA3424Ac51f8C1E586fd43aE226F') -- OP Foundation Locked Grants Wallet
) as v(address)