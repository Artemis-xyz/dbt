{{ config(materialized="table") }}

-- 1e6 (fees should be divided by 1e6)
with
    raw_transactions_with_columns as (
        select source_url, max_by(source_json, extraction_date) as source_json
        from {{ source("PROD_LANDING", "raw_stacks_transactions") }}
        group by source_url
    )
select
    value:"tx_id"::string as tx_id,
    value:"tx_index"::string as tx_index,
    value:"block_hash"::string as block_hash,
    value:"block_height"::number as block_height,
    value:"burn_block_time_iso"::timestamp_ntz as block_timestamp,
    value:"sender_address"::string as sender_address,
    value:"tx_status"::string as tx_status,
    value:"tx_type"::string as tx_type,
    coalesce(
        value:"contract_call",
        value:"token_transfer",
        value:"coinbase_payload",
        value:"smart_contract",
        value:"poison_microblock"
    ) as event_json,
    value:"fee_rate"::number / 1e6 as tx_fee
from raw_transactions_with_columns, lateral flatten(input => parse_json(source_json))
