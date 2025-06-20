{{config(materialized="incremental", snowflake_warehouse='USDT0', unique_key=["transaction_hash", "event_index"])}}
{{OFTSent('optimism', '0xF03b4d9AC1D5d1E7c4cEf54C2A313b9fe051A0aD', '0x01bFF41798a0BcF287b996046Ca68b395DbC1071', 'tether', 6)}}
