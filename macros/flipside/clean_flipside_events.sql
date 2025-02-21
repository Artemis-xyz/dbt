{% macro clean_flipside_evm_events(chain) %}
    select 
        concat(coalesce(substring(topic_1, 3), ''), coalesce(substring(topic_2, 3), ''), coalesce(substring(topic_3, 3),'')) as topic_data
        , SUBSTRING(data, 3) as data
        , block_number
        , null as block_hash
        , block_timestamp
        , tx_status
        , tx_succeeded
        , null as transaction_index 
        , tx_hash as transaction_hash       
        , event_index
        , contract_address
        , topics
        , topics[0]::string as topic_zero
    from {{ chain }}_flipside.core.fact_event_logs 
    {% if is_incremental() %}
    where
        block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
{% endmacro %}
