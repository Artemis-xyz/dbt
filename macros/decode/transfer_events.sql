{% macro transfer_events(chain) %}
    select
        block_timestamp,
        block_number,
        transaction_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        contract_address,
        decoded_log:"from"::string as "from",
        decoded_log:"to"::string as "to",
        decoded_log:"value"::string as amount,
        tx_status
    from {{ ref("fact_" ~ chain ~ "_decoded_events") }}
    where topic_zero = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    {% if is_incremental() %}
        and
            block_timestamp
            >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
{% endmacro %}
