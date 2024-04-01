{{ config(materialized="incremental", unique_key="block_number") }}
select
    block_number,
    block_timestamp,
    block_header_json:"gasUsed" * block_header_json:"baseFeePerGas" as block_burn
from polygon_flipside.core.fact_blocks
{% if is_incremental() %}
    where block_timestamp >= (select max(block_timestamp) from {{ this }})
{% endif %}
