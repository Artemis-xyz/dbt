{{config(materialized="incremental", snowflake_warehouse='USDT0', unique_key=["transaction_hash", "event_index"])}}
{{OFTSent('unichain', '0xc07bE8994D035631c36fb4a89C918CeFB2f03EC3', 'tether', 6)}}
