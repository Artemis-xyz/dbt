{{
    config(
        materialized="incremental",
        snowflake_warehouse="CELO",
        database="celo",
        schema="raw",
        alias="ez_transfers",
    )
}}

select
    block_timestamp,
    block_number,
    transaction_hash,
    index::string as event_index,
    contract_address,
    from_address,
    to_address,
    amount::number as amount_raw,
    amount_adjusted as amount_native, 
    amount_usd as amount,
    tx_status
from {{ref('fact_celo_native_token_transfers')}}
{% if is_incremental() %}
    where block_timestamp >= (select max(block_timestamp) from {{ this }})
    and contract_address = 'native-token:42220'
{% endif %}
union all
select
    block_timestamp,
    block_number,
    transaction_hash,
    event_index::string as event_index,
    contract_address,
    from_address,
    to_address,
    amount_raw,
    amount_native,
    amount,
    1 as tx_status
from {{ref('fact_celo_token_transfers')}}
{% if is_incremental() %}
    where block_timestamp >= (select max(block_timestamp) from {{ this }})
    and contract_address <> 'native-token:42220'
{% endif %}