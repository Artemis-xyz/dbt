{{
    config(
        materialized="incremental",
        unique_key=["transaction_hash", "event_index"],
        snowflake_warehouse="CELO_LG"
    )
}}

{{ clean_goldsky_events_v2("celo") }}
union all
select 
    block_number
    , block_timestamp
    , transaction_hash
    , -1 as transaction_index
    , event_index
    , contract_address
    , topic_zero
    , topics
    , replace(array_to_string(array_slice(topics, 1, array_size(topics)), ''), '0x', '') as topic_data
    , data
from {{ref("fact_celo_epoch_reward_events")}}
where status = 1
{% if is_incremental() %}
    and
        block_timestamp
        >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
{% endif %}
