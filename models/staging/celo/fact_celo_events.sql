-- depends_on: {{ ref('fact_celo_transactions') }}
{{
    config(
        materialized="incremental",
        unique_key=["transaction_hash", "event_index"],
    )
}}
{{
    unpack_logs_json(
        "celo",
        log_column_map=[
            ("transactionHash", to_string, "transaction_hash"),
            ("transactionIndex", hex_to_number, "transaction_index"),
            ("logIndex", hex_to_number, "event_index"),
            ("address", to_address, "contract_address"),
            ("topics", to_json, "topics"),
            ("data", to_string, "data"),
            ("removed", to_string, "removed"),
        ],
    )
}}
union all
select 
    block_timestamp,
    block_number,
    status,
    origin_from_address,
    origin_to_address,
    transaction_hash,
    -1 as transaction_index,
    event_index,
    contract_address,
    topics,
    replace(array_to_string(array_slice(topics, 1, array_size(topics)), ''), '0x', '') as topic_data,
    data,
    removed,
    topic_zero,
    event_data
from {{ref("fact_celo_epoch_reward_events")}}
{% if is_incremental() %}
    where
        block_timestamp
        >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
{% endif %}
