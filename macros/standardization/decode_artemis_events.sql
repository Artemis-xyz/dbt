{% macro decode_artemis_events(chain) %}
    with
    events as (
        select
            block_number
            , block_timestamp
            , transaction_hash
            , transaction_index
            , event_index
            , contract_address
            , topic_data
            , topic_zero
            , topics
            , data
            , array_size(topics) - 1 as indexed_topic_count
        from {{ ref("fact_" ~ chain ~ "_events") }}
        {% if is_incremental() %}
            where
                block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
    )
    select
        block_number
        , block_timestamp
        , transaction_hash
        , transaction_index
        , event_index
        , contract_address
        , topic_data
        , data
        , b.event_name
        , pc_dbt_db.prod.decode_evm_event_log_v5(event_info, data, topics) AS decoded_log_with_status
        , decoded_log_with_status[0] as decoded_log
        , decoded_log_with_status[1]::boolean as decoded_log_status
        , b.topic_zero
        , b.event_info
    from events a
    inner join {{ ref("dim_events_silver") }} b 
        -- We need to join on the number of indexed topics
        on a.topic_zero = b.topic_zero and a.indexed_topic_count = b.indexed_topic_count
{% endmacro %}
