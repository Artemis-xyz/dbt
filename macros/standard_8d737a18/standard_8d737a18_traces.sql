{% macro standard_8d737a18_traces(chain) %}
select 
    block_number
    , block_timestamp as transaction_timestamp
    , block_hash
    , transaction_hash as transaction_hash
    , transaction_index as transfer_index
    , from_address as sender_address
    , to_address as receiver_address
    , value
    , input
    , output
    , trace_type
    , call_type
    , reward_type
    , gas
    , gas_used
    , subtraces
    , trace_address
    , error
    , status
    , trace_id
    , '{{chain}}' as chain_name
    , ca.chain_agnostic_id as chain_id
    , id
from {{ref("fact_"~ chain ~ "_traces")}}
left join {{ ref("chain_agnostic_ids") }} ca
    on '{{chain}}' = ca.chain
{% if is_incremental() %} 
    where block_timestamp >= (
        select dateadd('day', -3, max(transaction_timestamp))
        from {{ this }}
    )
{% endif %}
{% endmacro %}
