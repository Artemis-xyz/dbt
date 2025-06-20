{% macro OFTSent(chain, contract_address, token_address, coingecko_id, decimals) %}

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
        , decoded_log:amountSentLD::number as amount_sent_ld
        , decoded_log:dstEid::number as dst_eid
        , decoded_log:fromAddress::string as src_address
        , decoded_log:guid::string as guid
    from {{ ref("fact_" ~ chain ~ "_decoded_events") }} a
    where lower(contract_address) = lower('{{ contract_address }}')
        and event_name = 'OFTSent'
)


select 
    block_number
    , block_timestamp
    , transaction_hash
    , transaction_index
    , event_index
    , contract_address
    , lower('{{token_address}}') as token_address

    , amount_received_ld as amount_received_raw
    , amount_received_ld / power(10, {{ decimals }}) as amount_received_native
    , amount_received_ld / power(10, {{ decimals }}) * prices.price as amount_received

    , amount_sent_ld as amount_sent_raw
    , amount_sent_ld / power(10, {{ decimals }}) as amount_sent_native
    , amount_sent_ld / power(10, {{ decimals }}) * prices.price as amount_sent
    , prices.price as price
    , dst_eid
    , dim_chain_id_mapping.chain_name as dst_chain
    , '{{ chain }}' as src_chain
    , src_address
    , guid
from events
left join prices
    on events.block_timestamp::date = prices.date
left join {{ ref("dim_chain_id_mapping") }} dim_chain_id_mapping
    on events.dst_eid = dim_chain_id_mapping.stargate_chain_id

{% endmacro %}
