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
    ('0xE4553b743E74dA3424Ac51f8C1E586fd43aE226F'), -- OP Foundation Locked Grants Wallet
    ('0x44A2CaD21c3FC84652C7675d8cdB2e871BC06cf4'), -- Wallet that receives large amounts of OP from the Foundation periodically for internal use or off-ramping
    ('0x19793c7824Be70ec58BB673CA42D2779d12581BE'), -- Wallet that receives large amounts of OP from the Foundation periodically for internal use or off-ramping
    ('0xb3C2f9fC2727078EC3A2255410e83BA5B62c5B5f'), -- Wallet that receives large amounts of OP from the Foundation periodically for internal use or off-ramping
    ('0xAcf32F4e1260636cf1e3066c060C74AD52fE4E9e'), -- Wallet that receives large amounts of OP from the Foundation periodically for internal use or off-ramping
    ('0xE74B1b7d78c180Ff937B464e544D0701038ACBF0'), -- Wallet that receives large amounts of OP from the Foundation periodically for internal use or off-ramping
    ('0x9a69d97a451643a0Bb4462476942D2bC844431cE') -- Wallet that receives large amounts of OP from the Foundation periodically for internal use or off-ramping
) as v(address)