{% macro OFTRecieved(chain, contract_address, coingecko_id, decimals) %}

with prices as (
    {{ get_coingecko_price_with_latest(coingecko_id) }}
)

, events as (
    select
        block_number
        , block_timestamp
        , transaction_hash
        , transaction_index
        , event_index
        , contract_address
        , decoded_log:amountReceivedLD::number as amount_received_ld
        , decoded_log:srcEid::number as src_eid
        , decoded_log:toAddress::string as to_address
        , decoded_log:guid::string as guid
    from {{ ref("fact_" ~ chain ~ "_decoded_events") }} a
    where lower(contract_address) = lower('{{ contract_address }}')
        and event_name = 'OFTReceived'
)

select 
    block_number
    , block_timestamp
    , transaction_hash
    , transaction_index
    , event_index
    , contract_address
    , amount_received_ld as amount_received_raw
    , amount_received_ld / power(10, {{ decimals }}) as amount_received_native
    , amount_received_ld / power(10, {{ decimals }}) * prices.price as amount_received
    , prices.price as price
    , src_eid
    , dim_chain_id_mapping.chain_name as src_chain
    , '{{ chain }}' as dst_chain
    , to_address
    , guid
from events
left join prices
    on events.block_timestamp::date = prices.date
left join {{ ref("dim_chain_id_mapping") }} dim_chain_id_mapping
    on events.src_eid = dim_chain_id_mapping.stargate_chain_id

{% endmacro %}