{{config(materialized='incremental', unique_key=['tx_hash', 'chain', 'event_index'], snowflake_warehouse="ACROSS_V2")}}

with filled_relay_events as (
    ({{ across_v3_decode_filled_relay('base', '0x09aea4b2242abc8bb4bb78d537a67a245a7bec64') }})
    union all
    ({{ across_v3_decode_filled_relay('ethereum', '0x5c7bcd6e7de5423a257d81b442095a1a6ced35c5') }})
    union all
    ({{ across_v3_decode_filled_relay('arbitrum', '0xe35e9842fceaCA96570B734083f4a58e8F7C5f2A') }})
    union all
    ({{ across_v3_decode_filled_relay('optimism', '0x6f26Bf09B1C792e3228e5467807a900A503c0281') }})
    union all
    ({{ across_v3_decode_filled_relay('polygon', '0x9295ee1d8C5b022Be115A2AD3c30C72E34e7F096') }})
    union all
    ({{ across_v3_goldsky_decode_filled_relay('ink', '0xeF684C38F94F48775959ECf2012D7E864ffb9dd4') }})
    union all
    ({{ across_v3_goldsky_decode_filled_relay('soneium', '0x3baD7AD0728f9917d1Bf08af5782dCbD516cDd96') }})
    union all
    ({{ across_v3_goldsky_decode_filled_relay('linea', '0x7E63A5f1a8F0B4d0934B2f2327DAED3F6bb2ee75') }})
    union all
    ({{ across_v3_goldsky_decode_filled_relay('worldchain', '0x09aea4b2242abC8bb4BB78D537A67a245A7bEC64') }})
    union all
    ({{ across_v3_goldsky_decode_filled_relay('unichain', '0x09aea4b2242abC8bb4BB78D537A67a245A7bEC64') }})
)
SELECT DISTINCT 
    messaging_contract_address,
    block_timestamp,
    tx_hash,
    event_index,
    deposit_id,
    origin_token,
    src_amount,
    dst_amount,
    depositor,
    recipient,
    ids.id as destination_chain_id,
    destination_token,
    origin_chain_id,
    realized_lp_fee_pct,
    relayer_fee_pct,
    protocol_fee,
    message,
    filled_relay_events.chain,
    decoded_log
FROM filled_relay_events
left join {{ ref('dim_chain_ids') }} as ids on filled_relay_events.chain = ids.chain
WHERE 
deposit_id is not null and origin_chain_id is not null
