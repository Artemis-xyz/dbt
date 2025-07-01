{{ config(snowflake_warehouse="SEI", materialized="incremental", unique_key="unique_id") }}

select
    block_timestamp,
    block_number,
    transaction_hash,
    contract_address,
    from_address,
    to_address,
    amount_raw,
    amount_native,
    amount,
    price,
    md5(transaction_hash || transaction_index || event_index || coalesce(from_address, '') || coalesce(to_address, '') || amount) as unique_id
from {{ ref('fact_sei_evm_token_transfers') }}
{% if is_incremental() %}
    where block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
{% endif %}
