{{ config(materialized="incremental", unique_key="block_number") }}

with
    tagged_ethereum_blocks as (
        select
            block_number,
            block_timestamp,
            gas_used * base_fee_per_gas as block_burn,
            block_hash AS hash,
            case when t2.name is null then 'Unknown' else t2.name end as builder_name,
            case
                when t2.censors is null then 'Non-Censoring' else t2.censors
            end as censors,
            case when t2.entity is null then 'Unknown' else t2.entity end as entity,
            case
                when t2.category is null then 'Block Proposer' else t2.category
            end as category,
            difficulty,
            extra_data,
            fact_blocks_id,
            gas_limit,
            gas_used,
            miner as builder,
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
        from ethereum_flipside.core.fact_blocks t1
        left join
            {{ ref("dim_builder_contracts_tagged") }} t2
            on lower(t1.miner) = lower(t2.address)
    )

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
from tagged_ethereum_blocks
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run 
    where block_timestamp >= (select max(block_timestamp) from {{ this }})
{% endif %}
