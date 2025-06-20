{{config(materialized="incremental", snowflake_warehouse='USDT0', unique_key=["transaction_hash", "event_index"])}}
{{OFTRecieved('ink', '0x1cB6De532588fCA4a21B7209DE7C456AF8434A65', 'tether', 6)}}

