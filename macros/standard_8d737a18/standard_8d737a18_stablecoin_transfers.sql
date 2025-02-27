{% macro standard_8d737a18_stablecoin_transfers(chain) %}
select
    block_timestamp as transaction_timestamp
    , date::date as date_day
    , block_number
    , index as transfer_index
    , tx_hash as transaction_hash
    , from_address as sender_address
    , to_address as receiver_address
    , is_mint
    , is_burn
    , amount as amount_asset
    , inflow
    , transfer_volume
    , ca.chain_agnostic_id || ':' || replace(replace(contract_address, '0x', ''), '0:', '') as asset_id as asset_id
    , symbol as asset_symbol
    , '{{chain}}' as chain_name
    , ca.chain_agnostic_id as chain_id
from {{ ref( "fact_"~ chain ~ "_stablecoin_transfers") }} st
left join {{ ref("chain_agnostic_ids") }} ca
    on '{{chain}}' = ca.chain
{% if is_incremental() %} 
    where block_timestamp >= (
        select dateadd('day', -3, max(block_timestamp))
        from {{ this }}
    )
{% endif %}
{% endmacro %}
