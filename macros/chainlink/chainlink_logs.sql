{% macro chainlink_logs(chain, topics) %}
    select
        '{{ chain }}' as chain
        , contract_address
        , decoded_log
        , tx_hash
        , block_number
        , block_timestamp
        , event_index
        , origin_from_address as tx_from
        , event_name
        , tx_succeeded AS tx_status
    from {{ chain }}_flipside.core.ez_decoded_event_logs
    {% if topics is string %} where topics[0]::string = '{{ topics }}'
    {% else %} where topics[0]::string in {{ topics }}
    {%endif%}
    {% if is_incremental() %}
        and block_timestamp >= (select max(block_timestamp) from {{ this }})
    {% endif %}
{% endmacro %}