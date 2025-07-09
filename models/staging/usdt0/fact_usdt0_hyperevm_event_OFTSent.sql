{{config(materialized="incremental", snowflake_warehouse='USDT0', unique_key=["transaction_hash", "event_index"])}}
{{OFTSent('hyperevm', '0x904861a24F30EC96ea7CFC3bE9EA4B476d237e98', '0xB8CE59FC3717ada4C02eaDF9682A9e934F625ebb', 'tether', 6)}}
