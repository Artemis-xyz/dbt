{% macro chainalaysis_traces(chain) %}

{% set chain_name = '' %}
{% if chain == 'celo' %}
    {% set chain_name = 'eip155:42220' %}
{% elif chain == 'solana' %}
    {% set chain_name = 'solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp' %}
{% elif chain == 'ton' %}
    {% set chain_name = 'tvm:-239' %}
{% endif %}


{% if chain_name == '' %}
  {{ exceptions.raise_compiler_error("Error: chain_name is not set. Please update marco to include mapping of " ~ chain) }}
{% endif %}

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
    , '{{chain_name}}' as chain_name
    , id
from {{ref("fact_"~ chain ~ "_traces")}}
{% endmacro %}
