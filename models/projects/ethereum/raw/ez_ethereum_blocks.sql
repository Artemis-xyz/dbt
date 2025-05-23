-- depends_on: {{ ref('fact_ethereum_blocks') }}
{{
    config(
        materialized="incremental",
        unique_key="block_number",
        snowflake_warehouse="CHAIN_BLOCKS_XS",
        database="ethereum",
        schema="raw",
        alias="ez_blocks",
    )
}}

select
    block_number,
    block_timestamp,
    block_burn,
    builder_name,
    'ethereum' as chain,
    censors,
    entity,
    category,
    difficulty,
    extra_data,
    fact_blocks_id,
    gas_limit,
    gas_used,
    hash,
    builder,
    network,
    nonce,
    parent_hash,
    receipts_root,
    sha3_uncles,
    size,
    tx_count,
    uncle_blocks,
    withdrawals,
    withdrawals_root
from {{ ref("fact_ethereum_blocks") }}
{% if is_incremental() %}
    where block_number > (select max(block_number) from {{ this }})
{% endif %}
