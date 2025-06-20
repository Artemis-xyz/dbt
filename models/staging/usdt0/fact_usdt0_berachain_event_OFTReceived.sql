{{config(materialized="incremental", snowflake_warehouse='USDT0', unique_key=["transaction_hash", "event_index"])}}
{{OFTRecieved('berachain', '0x3Dc96399109df5ceb2C226664A086140bD0379cB', '0x779Ded0c9e1022225f8E0630b35a9b54bE713736', 'tether', 6)}}

