{{config(materialized="incremental", snowflake_warehouse='USDT0', unique_key=["transaction_hash", "event_index"])}}
{{OFTSent('unichain', '0xc07bE8994D035631c36fb4a89C918CeFB2f03EC3', '0x9151434b16b9763660705744891fA906F660EcC5', 'tether', 6)}}
