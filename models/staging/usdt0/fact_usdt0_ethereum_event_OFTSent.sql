{{config(materialized="incremental", snowflake_warehouse='USDT0', unique_key=["transaction_hash", "event_index"])}}
{{OFTSent('ethereum', '0x6C96dE32CEa08842dcc4058c14d3aaAD7Fa41dee', '0xdac17f958d2ee523a2206206994597c13d831ec7', 'tether', 6)}}
