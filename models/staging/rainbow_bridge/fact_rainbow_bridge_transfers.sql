{{ config(
    materialized="incremental",
    unique_key="ez_bridge_activity_id",
) }}
with rainbow_bridge_transfers as (
    select 
        block_timestamp
        , tx_hash
        , source_chain
        , destination_chain
        , destination_address as recipient
        , source_address as depositor
        , case 
            when token_address = 'aurora' then symbol
            else token_address
        end as token_address
        , amount
        , amount_usd
        , ez_bridge_activity_id
    from near_flipside.defi.ez_bridge_activity 
    where platform = 'rainbow' and RECEIPT_SUCCEEDED
        and tx_hash not in ('qrJ4Hwh4xiPHnfWQC7PeXEinspBu1SkJvz3qbPRaGT8', 'DA4UvCkTrJ5py6H7M5RMj4RgdG8r9LNReVQKtWtsYvDy')
    {% if is_incremental() %}
        and block_timestamp > (select max(block_timestamp) from {{this}})
    {% endif %}
)
select 
    block_timestamp,
    tx_hash,
    source_chain,
    destination_chain,
    recipient,
    depositor,
    token_address,
    amount,
    amount_usd,
    ez_bridge_activity_id
from rainbow_bridge_transfers