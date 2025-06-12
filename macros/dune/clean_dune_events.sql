{% macro clean_dune_evm_events(chain) %}
    select 
        block_number
        , CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', block_time) as block_timestamp
        , tx_hash_hex as transaction_hash
        , tx_index as transaction_index
        , index as event_index
        , contract_address_hex as contract_address
        , topic0_hex as topic_zero
        , ARRAY_COMPACT(ARRAY_CONSTRUCT(topic0_hex, topic1_hex, topic2_hex, topic3_hex)) AS topics
        , nullif(concat(coalesce(substring(topic1_hex, 3), ''), coalesce(substring(topic2_hex, 3), ''), coalesce(substring(topic3_hex, 3),'')), '') as topic_data
        , data_hex as data
    from zksync_dune.{{chain}}.logs
    {% if is_incremental() %}
        where block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
{% endmacro %}
