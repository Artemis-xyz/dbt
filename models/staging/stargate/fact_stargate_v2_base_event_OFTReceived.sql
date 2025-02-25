{{config(materialized="incremental", snowflake_warehouse='STARGATE_MD', unique_key=["tx_hash", "event_index"])}}

{{stargate_OFTReceived('base')}}

--OFTReceived
-- {
--   "amountReceivedLD": 1345218498,
--   "guid": "0xe01fa07ba68c0f8ee153060c9cb43e92a654614ef22fda29e97072eb7a38bc07",
--   "srcEid": 30110,
--   "toAddress": "0x1f6de1830492394f2ba59814e4492957390d9088"
-- }

