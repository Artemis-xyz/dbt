{% macro get_ez_decoded_events(chain) %}
    select
        block_timestamp,
        block_number,
        transaction_hash,
        event_index,
        contract_address,
        origin_from_address,
        origin_to_address,
        status,
        event_name,
        decoded_event
    from {{ ref("fact_" ~ chain ~ "_decoded_events") }}
{% endmacro %}