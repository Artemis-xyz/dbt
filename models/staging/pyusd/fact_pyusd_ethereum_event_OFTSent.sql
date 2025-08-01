{{config(materialized="incremental", snowflake_warehouse='PYUSD', unique_key=["transaction_hash", "event_index"])}}

with 
PYUSD_Locker as (
{{OFTSent('ethereum', '0xFA0e06B54986ad96DE87a8c56Fea76FBD8d493F8', '0x6c3ea9036406852006290770bedfcaba0e23a0e8', 'paypal-usd', 6)}}
)
, PYUSD_OFT_Adapter as (
{{OFTSent('ethereum', '0x688e72142674041f8f6Af4c808a4045cA1D6aC82', '0x6c3ea9036406852006290770bedfcaba0e23a0e8', 'paypal-usd', 6)}}
)
select * from PYUSD_Locker
union all
select * from PYUSD_OFT_Adapter