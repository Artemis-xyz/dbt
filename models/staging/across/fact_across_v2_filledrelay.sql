{{config(materialized='incremental', unique_key=['tx_hash', 'chain', 'event_index'], snowflake_warehouse="ACROSS_V2")}}

({{ across_v2_decode_filled_relay('base', '0x09aea4b2242abc8bb4bb78d537a67a245a7bec64') }})
union all
({{ across_v2_decode_filled_relay('ethereum', '0x4d9079bb4165aeb4084c526a32695dcfd2f77381') }})
union all
({{ across_v2_decode_filled_relay('arbitrum', '0xb88690461ddbab6f04dfad7df66b7725942feb9c') }})
union all
({{ across_v2_decode_filled_relay('optimism', '0x59485d57eecc4058f7831f46ee83a7078276b4ae') }})
union all
({{ across_v2_decode_filled_relay('polygon', '0x69b5c72837769ef1e7c164abc6515dcff217f920') }})
