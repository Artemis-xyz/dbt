{{config(materialized="incremental", snowflake_warehouse='USDT0', unique_key=["transaction_hash", "event_index"])}}
{{OFTSent('ethereum', '0x6C96dE32CEa08842dcc4058c14d3aaAD7Fa41dee', 'tether', 6)}}
