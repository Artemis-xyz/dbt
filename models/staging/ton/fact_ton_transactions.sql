{{
    config(
        materialized="incremental",
        unique_key="tx_id",
    )
}}
with raw_data as (
    select
        extraction_date
        , parse_json(source_json) as source_json
        , parse_json(source_json):"decoded_hash"::string as tx_hash
        , parse_json(source_json):"tx_id"::string as tx_id
        , TO_TIMESTAMP(parse_json(source_json):"utime") as block_timestamp
        , parse_json(source_json):"total_fees"::bigint as total_fees
        , parse_json(source_json):"storage_ph_fees_collected"::bigint as storage_ph_fees_collected
        , parse_json(source_json):"success"::boolean as success
        , parse_json(source_json):"in_msg"::string as in_msg
        , parse_json(source_json):"lt"::bigint as lt
        , parse_json(source_json):"transaction_account"::string as transaction_account
        , parse_json(source_json):"transaction_account_workchain"::int as transaction_account_workchain
        , parse_json(source_json):"transaction_account_interfaces"::array as transaction_account_interfaces
        , parse_json(source_json):"from_address"::string as from_address
        , parse_json(source_json):"from_workchain"::int as from_workchain
        , parse_json(source_json):"from_interfaces"::array as from_interfaces
        , parse_json(source_json):"to_address"::string as to_address
        , parse_json(source_json):"to_workchain"::int as to_workchain
        , parse_json(source_json):"to_interfaces"::array as to_interfaces
        , parse_json(source_json):"trace_id"::string as trace_id
        , parse_json(source_json):"op_code"::string as op_code
        , parse_json(parse_json(source_json):"decoded_body") as decoded_body
        , parse_json(source_json):"init"::string as init
        , parse_json(source_json):"value"::bigint as value
        , parse_json(source_json):"msg_type"::string as msg_type
        , parse_json(source_json):"init_interfaces"::array as init_interfaces
    from
        {{ source("PROD_LANDING", "raw_ton_transactions") }}
    {% if is_incremental() %}
        where TO_TIMESTAMP(parse_json(source_json):"utime") > (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        and extraction_date > (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
)
select 
    max_by(tx_hash, extraction_date) as tx_hash
    , tx_id
    , max_by(block_timestamp, extraction_date) as block_timestamp
    , max_by(total_fees, extraction_date) as total_fees
    , max_by(storage_ph_fees_collected, extraction_date) as storage_ph_fees_collected
    , max_by(success, extraction_date) as success
    , max_by(in_msg, extraction_date) as in_msg
    , max_by(lt, extraction_date) as lt
    , max_by(transaction_account, extraction_date) as transaction_account
    , max_by(transaction_account_workchain, extraction_date) as transaction_account_workchain
    , max_by(transaction_account_interfaces, extraction_date) as transaction_account_interfaces
    , max_by(from_address, extraction_date) as from_address
    , max_by(from_workchain, extraction_date) as from_workchain
    , max_by(from_interfaces, extraction_date) as from_interfaces
    , max_by(to_address, extraction_date) as to_address
    , max_by(to_workchain, extraction_date) as to_workchain
    , max_by(to_interfaces, extraction_date) as to_interfaces
    , max_by(trace_id, extraction_date) as trace_id
    , max_by(op_code, extraction_date) as op_code
    , max_by(decoded_body, extraction_date) as decoded_body
    , max_by(init, extraction_date) as init
    , max_by(value, extraction_date) as value
    , max_by(msg_type, extraction_date) as msg_type
    , max_by(init_interfaces, extraction_date) as init_interfaces
from raw_data
group by tx_id
