{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
    )
}}
select distinct
    avro_raw:hash::string as tx_hash
    , avro_raw:trace_id::string as trace_id
    , avro_raw:now::timestamp as block_timestamp 
    , block_timestamp::date as raw_date
    , avro_raw:total_fees::float as total_fees
    , avro_raw:storage_fees_collected::float as storage_ph_fees_collected
    , avro_raw:lt::bigint as lt
    , avro_raw:account::string as transaction_account
    , avro_raw:block_workchain::number as transaction_account_workchain 
    , avro_raw:account_state_code_hash_after::string as account_code_hash
    , interface as transaction_account_interface
    , not avro_raw:aborted::boolean as success
from {{ source("PROD_LANDING", "raw_ton_transactions_avro") }}
left join {{ref("ton_tagged_interfaces")}} on account_code_hash = code_hash
where avro_raw:now::timestamp::date >= '2022-06-28'
{% if is_incremental() %}
    and avro_raw:now::timestamp > (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
{% endif %}