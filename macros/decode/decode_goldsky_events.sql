{% macro decode_goldsky_events(chain) %}
    select
        block_number
        , block_hash
        , block_timestamp
        , transaction_hash
        , transaction_index
        , event_index
        , contract_address
        , topic_data
        , data
        , t2.event_name
        , {{ target.schema }}.decode_evm_event_log_v2(
            topic_data, data, t2.event_info
        ) as decoded_log
        , t2.topic_zero
        , t2.event_info
    from {{ ref("fact_" ~ chain ~ "_events") }} t1
    left join {{ ref("dim_events_silver") }} t2 on t1.topic_zero = t2.topic_zero
    {% if is_incremental() %}
        where
            block_timestamp
            >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
{% endmacro %}
