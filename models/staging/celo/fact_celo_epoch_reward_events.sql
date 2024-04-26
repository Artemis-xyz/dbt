{{ 
    config(
        materialized="table",
        unique_key=["transaction_hash", "event_index"],
        snowflake_warehouse="CELO", 
    )
}}
with
    lastest_data as (
        select
            epoch_blocks as block_number,
            max_by(source_json, extraction_date) as latest_data
        from {{ source("PROD_LANDING", "raw_celo_epoch_rewards") }}

        {% if is_incremental() %}
            where epoch_blocks >= (select max(epoch_blocks) - 10 from {{ this }})
        {% endif %}
        group by block_number
    ),
    upacked_data as (
        select
            block_number,
            value:"blockHash"::string as block_hash,
            null as origin_from_address,
            null as origin_to_address,
            value:"transactionHash"::string as transaction_hash,
            value:"address"::string as contract_address,
            value:"data"::string as data,
            value:"topics"::variant as topics,
            topics[0]::string as topic_zero,
            {{ target.schema }}.concat_topics_and_data(topics, data) as event_data,
            {{ target.schema }}.hex_to_int(value:"logIndex"::string)::number as event_index,
            value:"removed"::string as removed,
            1 as status
        from lastest_data t1, lateral flatten(input => latest_data:"logs")
    )

select
    t2.block_timestamp,
    t1.block_number,
    t1.status,
    t1.origin_from_address,
    t1.origin_to_address,
    t1.transaction_hash,
    t1.event_index,
    t1.contract_address,
    t1.topics,
    t1.data,
    t1.removed,
    t1.topic_zero,
    t1.event_data
from upacked_data t1
left join {{ ref("fact_celo_blocks") }} t2 using (block_number)
where block_hash = transaction_hash