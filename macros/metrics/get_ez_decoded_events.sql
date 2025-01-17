{% macro get_ez_decoded_events(chain) %}
    select
        block_timestamp,
        block_number,
        transaction_hash,
        event_index,
        contract_address,
        origin_from_address,
        origin_to_address,
        tx_status,
        event_name,
        decoded_log,
        topic_zero,
        data
    from {{ ref("fact_" ~ chain ~ "_decoded_events") }}
{% endmacro %}