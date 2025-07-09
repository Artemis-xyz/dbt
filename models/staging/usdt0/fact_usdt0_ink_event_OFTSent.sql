{{config(materialized="incremental", snowflake_warehouse='USDT0', unique_key=["transaction_hash", "event_index"])}}
{{OFTSent('ink', '0x1cB6De532588fCA4a21B7209DE7C456AF8434A65', '0x0200C29006150606B650577BBE7B6248F58470c1', 'tether', 6)}}
