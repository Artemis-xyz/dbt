{% macro stargate_OFTSent(chain)%}
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
        , 'OFTSent' as event_name
        , '{{ chain }}' as src_chain
        , pc_dbt_db.prod.hex_to_int(substr(data, 67, 64))::bigint as amount_sent_ld
        , pc_dbt_db.prod.hex_to_int(substr(data, 0, 66))::bigint as dst_e_id
        , '0x' || substr(topics[2]::string::string, 27, 40) as src_address
        , topics[1]::string as guid
        , token_messaging_address
        , stargate_implementation_pool
        , token_address
        , decimals
        , symbol
        , tx_status
    from {{chain}}_flipside.core.fact_event_logs
    inner join {{ref("dim_stargate_v2_"~ chain~"_assets")}} on lower(contract_address) = lower(stargate_implementation_pool)
    where topics [0] :: STRING = '0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a' 
        {% if is_incremental() %}
            and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
)
select
    events.block_timestamp
    , events.tx_hash
    , events.event_index
    , events.event_name
    , events.src_chain
    , events.dst_e_id
    , events.src_address
    , events.guid
    , events.token_messaging_address
    , events.stargate_implementation_pool
    , events.token_address
    , events.decimals
    , events.symbol
    , amount_sent_ld as amount_sent_native
    , amount_sent_ld / pow(10, events.decimals) as amount_sent_adjusted
    , price * amount_sent_ld / pow(10, events.decimals) as amount_sent
    , events.tx_status
from events
left join prices on block_timestamp::date = prices.date and lower(prices.contract_address) = lower(token_address)
{% endmacro %}