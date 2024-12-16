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
    origin_from_address,
    origin_to_address,
    contract_address,
    from_address,
    to_address,
    amount::number as amount,
    amount_adjusted,
    amount_usd,
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
    origin_from_address,
    origin_to_address,
    contract_address,
    from_address,
    to_address,
    amount::number as amount,
    amount_adjusted,
    amount_usd,
    tx_status
from {{ref('fact_celo_token_transfers')}}
{% if is_incremental() %}
    where block_timestamp >= (select max(block_timestamp) from {{ this }})
    and contract_address <> 'native-token:42220'
{% endif %}