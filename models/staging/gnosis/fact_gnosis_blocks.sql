{{ config(materialized="incremental", unique_key="block_number") }}
select
    block_number,
    block_timestamp,
    gas_used * base_fee_per_gas as block_burn
from gnosis_flipside.core.fact_blocks
{% if is_incremental() %}
    where block_timestamp >= (select max(block_timestamp) from {{ this }})
{% endif %}
