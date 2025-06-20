{{config(materialized="incremental", snowflake_warehouse='USDT0', unique_key=["transaction_hash", "event_index"])}}
{{OFTRecieved('hyperevm', '0x904861a24F30EC96ea7CFC3bE9EA4B476d237e98', 'tether', 6)}}

