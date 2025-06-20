{{config(materialized="incremental", snowflake_warehouse='USDT0', unique_key=["transaction_hash", "event_index"])}}
{{OFTSent('berachain', '0x3Dc96399109df5ceb2C226664A086140bD0379cB', 'tether', 6)}}
