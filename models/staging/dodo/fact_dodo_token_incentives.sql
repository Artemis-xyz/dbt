{{ config(materialized="table") }}

with DODOClaims as (
    select
        block_timestamp,
        tx_hash,
        decoded_log:amount::NUMBER as amount,
        decoded_log:user::STRING as recipient,
        (decoded_log:amount::NUMBER / POW(10, decimals)) * price as amount_usd
    from {{ source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs') }}
    left join {{ source('ETHEREUM_FLIPSIDE_PRICE', 'ez_prices_hourly') }}
        on token_address = lower('0x43dfc4159d86f3a37a5a4b3d4580b888ad7d4ddd')
        and hour = date_trunc('hour', block_timestamp)
    where contract_address in (
        lower('0xaed7384f03844af886b830862ff0a7afce0a632c'),
        lower('0x1a4f8705e1c0428d020e1558a371b7e6134455a2'),
        lower('0x2ff2cee6e9359f9ea1cf2f51d18bf9f2045447e4'),
        lower('0x48672333f97958e2f8352b3a5538293de8ea86f7'),
        lower('0x44024b60575cf5d032f80a55da37924f123b4151'),
        lower('0x53ee28b9f0a6416857c1e7503032e27e80f52da0'),
        lower('0x136829c258e31b3ab1975fe7d03d3870c3311651'), --Addresses from 
        lower('0x748f4dff5996711a3e127aaba2e9b95347ccdc74'),
        lower('0xf9b8500b5012c059f30daa734d3a7131d668b1cd'),
        lower('0x3552fac00722ad60437ac173cda332acf4136810')
    )   
    and event_name = 'Claim'
) select * from DODOClaims