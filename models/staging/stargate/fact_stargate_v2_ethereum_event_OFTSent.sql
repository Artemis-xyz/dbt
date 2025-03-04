{{config(materialized="incremental", snowflake_warehouse='STARGATE_MD', unique_key=["tx_hash", "event_index"])}}
{{stargate_OFTSent('ethereum')}}
--OFTSent
-- {
--   "amountReceivedLD": 19989880098,
--   "amountSentLD": 19990079999,
--   "dstEid": 30110,
--   "fromAddress": "0xca077a26544657b08508e1f02236fc10bac86bf3",
--   "guid": "0x5da06535d326cf6e1c98aac52c1a467664e7aec5f91d3bbeb123445a006c9db5"
-- }
