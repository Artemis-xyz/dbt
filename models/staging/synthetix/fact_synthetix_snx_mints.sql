{{ config(materialized="table", snowflake_warehouse="SYNTHETIX") }}

with flattened_logs as (
    select 
        logs.block_timestamp,
        flat.value::number as supply_minted
    from ethereum_flipside.core.ez_decoded_event_logs as logs,
        lateral flatten(input => logs.decoded_log) as flat
    where 
        logs.event_name = 'SupplyMinted'
        and lower(logs.contract_address) in (
            lower('0xa05e45396703babaa9c276b5e5a9b6e2c175b521'), 
            lower('0x8d203c458d536fe0f97e9f741bc231eac8cd91cf')
        )
        and flat.key = 'supplyMinted'
)

select
    date(f.block_timestamp) as date,
    sum((f.supply_minted::number) / 1e18) as mints_native,
    sum(((f.supply_minted::number) / 1e18) * eph.price) as mints
from 
    flattened_logs f
join 
    ethereum_flipside.price.ez_prices_hourly as eph
    on lower(eph.symbol) = lower('SNX')
    and eph.hour = date_trunc('hour', f.block_timestamp)
group by date
order by date desc
