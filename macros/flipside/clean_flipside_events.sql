{% macro clean_flipside_evm_events(chain) %}
    select 
        block_number
        , block_timestamp
        , tx_hash as transaction_hash
        , tx_position as transaction_index 
        , event_index
        , contract_address
        , topic_0 as topic_zero
        , topics
        , concat(coalesce(substring(topic_1, 3), ''), coalesce(substring(topic_2, 3), ''), coalesce(substring(topic_3, 3),'')) as topic_data
        , SUBSTRING(data, 3) as data
    from {{ chain }}_flipside.core.fact_event_logs 
    left join {{ chain }}_flipside.core.fact_transactions using (tx_hash)
    {% if is_incremental() %}
    where block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
{% endmacro %}
