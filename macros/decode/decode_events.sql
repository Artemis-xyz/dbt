{% macro decode_events(chain) %}
    select
        block_timestamp,
        block_number,
        transaction_hash,
        event_index,
        contract_address,
        origin_from_address,
        origin_to_address,
        status as tx_status,
        t1.topic_zero,
        t2.event_name,
        {{ target.schema }}.decode_evm_event_log_v2(
            topic_data,  REPLACE(data, '0x', ''), t2.event_info
        ) as decoded_log,
        data
    from {{ ref("fact_" ~ chain ~ "_events") }} t1
    inner join {{ ref("dim_events_silver") }} t2 on t1.topic_zero = t2.topic_zero
    where tx_status = 1
    {% if is_incremental() %}
        where
            block_timestamp
            >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
{% endmacro %}
