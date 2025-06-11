{{ config(materialized="incremental", unique_key="unique_id") }}

with
    duped_data as (
        select
            source_json,
            extraction_date,
            source_url,
            source_json:transaction_hash as transaction_hash,
            source_json:log_index as log_index
        from {{ source("PROD_LANDING", "raw_tron_erc20_transfer_decoded_logs") }}
        {% if is_incremental() %}
            where
                extraction_date >= (
                    select dateadd('day', -3, max(trunc(block_timestamp, 'day')))
                    from {{ this }}
                )
        {% endif %}
    ),
    data as (
        select
            max_by(source_json, extraction_date) as source_json,
            max(extraction_date) as extraction_date,
            max(source_url) as source_url
        from duped_data
        group by transaction_hash, log_index
    )
select
    source_json:"transaction_hash"::string
    || '-'
    || source_json:"log_index"::string as unique_id,
    source_json:"address"::string as address,
    source_json:"block_hash"::string as block_hash,
    source_json:"block_number"::int as block_number,
    (source_json:"block_timestamp" / 1000)::varchar::timestamp as block_timestamp,
    source_json:"data"::string as data,
    source_json:"log_index"::int as log_index,
    source_json:"removed"::boolean as removed,
    source_json:"topics" as topics,
    source_json:"transaction_hash"::string as transaction_hash,
    source_json:"decoded_event_logs"::object as decoded_event_logs
from data
