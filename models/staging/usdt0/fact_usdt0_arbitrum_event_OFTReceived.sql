{{config(materialized="incremental", snowflake_warehouse='USDT0', unique_key=["transaction_hash", "event_index"])}}
{{OFTRecieved('arbitrum', '0x14E4A1B13bf7F943c8ff7C51fb60FA964A298D92', '0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9', 'tether', 6)}}

