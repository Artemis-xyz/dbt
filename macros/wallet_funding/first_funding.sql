{% macro first_funding(chain) %}

    with first_trace as (
        select
            block_timestamp
            , to_address as recipient
            , from_address as funder
            , tx_hash
            , value 
            , row_number() over (partition by to_address order by block_timestamp, trace_index) as rn 
        from {{ chain }}_flipside.core.fact_traces
        where trace_succeeded = true 
        and value > 0
        {% if is_incremental() %}
            -- only grab new traces
            and block_timestamp >= (select max(block_timestamp) from {{ this }})
            -- only grab traces for recipients who have not been logged yet
            and to_address not in (select distinct recipient from {{ this }})
        {% endif %}
    )

    select 
        block_timestamp
        , recipient
        , funder
        , tx_hash
        , value
        , '0x0000000000000000000000000000000000000000' as token_address
        , '{{ chain }}' as chain
    from first_trace
    where rn = 1

{% endmacro %}
