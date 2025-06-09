{% macro decode_artemis_events(chain) %}
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
        , pc_dbt_db.prod.decode_evm_event_log_v3(event_info, data, topics) AS decoded_log_with_status
        , decoded_log_with_status[0] as decoded_log
        , decoded_log_with_status[1]::boolean as decoded_log_status
        , b.topic_zero
        , b.event_info
    from {{ ref("fact_" ~ chain ~ "_events") }} a
    inner join {{ ref("dim_events_silver") }} b 
        on a.topic_zero = b.topic_zero
    {% if is_incremental() %}
        where
            block_timestamp
            >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
{% endmacro %}
