{{config(materialized="incremental", snowflake_warehouse='USDT0', unique_key=["transaction_hash", "event_index"])}}
{{OFTRecieved('optimism', '0xF03b4d9AC1D5d1E7c4cEf54C2A313b9fe051A0aD', 'tether', 6)}}

