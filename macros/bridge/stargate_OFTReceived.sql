{% macro stargate_OFTReceived(chain)%}
with 
prices as (
    select
        hour::date as date
        , token_address as contract_address
        , avg(price) as price
    from {{chain}}_flipside.price.ez_prices_hourly
    group by 1, 2
)
, events as (
    select 
        block_timestamp
        , tx_hash
        , event_index
        , 'OFTReceived' as event_name
        , '{{ chain }}' as dst_chain
        , pc_dbt_db.prod.hex_to_int(substr(data, 67))::bigint as amount_received_ld
        , pc_dbt_db.prod.hex_to_int(substr(data, 0, 66))::bigint as src_e_id
        , '0x' || substr(topics[2]::string::string, 27, 40) as dst_address
        , topics[1]::string as guid
        , token_messaging_address
        , stargate_implementation_pool
        , token_address
        , decimals
        , symbol
        , tx_status
    from {{chain}}_flipside.core.fact_event_logs
    inner join {{ref("dim_stargate_v2_"~ chain ~"_assets")}} on lower(contract_address) = lower(stargate_implementation_pool)
    where topics [0] :: STRING = '0xefed6d3500546b29533b128a29e3a94d70788727f0507505ac12eaf2e578fd9c'
    {% if is_incremental() %}
        and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
)
select
    events.block_timestamp
    , events.tx_hash
    , events.event_index
    , events.event_name
    , events.dst_chain
    , events.src_e_id
    , events.dst_address
    , events.guid
    , events.token_messaging_address
    , events.stargate_implementation_pool
    , events.token_address
    , events.decimals
    , events.symbol
    , amount_received_ld as amount_received_native
    , amount_received_ld / pow(10, events.decimals) as amount_received_adjusted
    , price * amount_received_ld / pow(10, events.decimals) as amount_received
    , events.tx_status
from events
left join prices on block_timestamp::date = prices.date and lower(prices.contract_address) = lower(token_address)
{% endmacro %}