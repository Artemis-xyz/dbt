{{ config(
    materialized="table",
    unique_key="tx_hash",
    snowflake_warehouse="TON",
) }}

with
    raw_receipt_transactions as (
        select
          avro_raw:utime::timestamp as block_timestamp
          , avro_raw:trace_id::string as trace_id
          , avro_raw:tx_hash::string as tx_hash
          , avro_raw:type::string as type
          , avro_raw:jetton_master::string as contract_address
          , avro_raw:jetton_wallet::string as jetton_wallet
          , avro_raw:source::string as from_address
          , avro_raw:destination::string as to_address
          , avro_raw:amount::float as amount
          , avro_raw:comment::string as comment
          , avro_raw:custom_payload::string as custom_payload
          , avro_raw:forward_payload::string as forward_payload
          , avro_raw:forward_ton_amount::float as forward_ton_amount
          , avro_raw:query_id::string as query_id
          , avro_raw:response_destination as response_destination
          , not avro_raw:tx_aborted::boolean as tx_status
          , avro_raw:tx_lt::bigint as event_index
        from {{ source("PROD_LANDING", "raw_ton_jetton_events_avro") }}
    )
select *
from raw_receipt_transactions
where tx_status = 'TRUE'
qualify row_number() over (partition by tx_hash order by block_timestamp desc) = 1