{{
    config(
        materialized="table",
        snowflake_warehouse="EXACTLY"
    )
}}

select
    date_trunc('day', date) as date,
    from_address,
    symbol as token,
    transfer_volume
from {{ ref("fact_optimism_stablecoin_transfers") }}
where
    lower(contract_address) = lower('0x0b2c639c533813f4aa9d7837caf62653d097ff85')
and
    lower(to_address) in (select lower(address) from {{ ref("fact_exactly_addresses") }})