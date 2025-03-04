{% macro stargate_OFTReceived(chain)%}

with 
usdc_prices as (
    select date as date, 'usdc' as symbol, shifted_token_price_usd as price
    from pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold
    where
        coingecko_id = 'usd-coin'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select dateadd('day', -1, to_date(sysdate())) as date, 'usdc' as symbol, token_current_price as price
    from pc_dbt_db.prod.fact_coingecko_token_realtime_data
    where token_id = 'usd-coin'
)
, eth_prices as (
    select date as date, 'eth' as symbol, shifted_token_price_usd as price
    from pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold
    where
        coingecko_id = 'ethereum'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select dateadd('day', -1, to_date(sysdate())) as date, 'eth' as symbol, token_current_price as price
    from pc_dbt_db.prod.fact_coingecko_token_realtime_data
    where token_id = 'ethereum'
)
, meth_prices as (
    select date as date, 'meth' as symbol, shifted_token_price_usd as price
    from pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold
    where
        coingecko_id = 'mantle-staked-ether'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select dateadd('day', -1, to_date(sysdate())) as date, 'meth' as symbol, token_current_price as price
    from pc_dbt_db.prod.fact_coingecko_token_realtime_data
    where token_id = 'mantle-staked-ether'
)
, usdt_prices as (
    select date as date, 'usdt' as symbol, shifted_token_price_usd as price
    from pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold
    where
        coingecko_id = 'tether'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select dateadd('day', -1, to_date(sysdate())) as date, 'usdt' as symbol, token_current_price as price
    from pc_dbt_db.prod.fact_coingecko_token_realtime_data
    where token_id = 'tether'
)
, bsc_usd_prices as (
    select date as date, 'bsc-usd' as symbol, shifted_token_price_usd as price
    from pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold
    where
        coingecko_id = 'binance-peg-busd'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select dateadd('day', -1, to_date(sysdate())) as date, 'bsc-usd' as symbol, token_current_price as price
    from pc_dbt_db.prod.fact_coingecko_token_realtime_data
    where token_id = 'binance-peg-busd'
)
, prices as (
    select date, symbol, price
    from usdc_prices
    union all
    select date, symbol, price
    from eth_prices
    union all
    select date, symbol, price
    from usdt_prices
    union all
    select date, symbol, price
    from bsc_usd_prices
    union all
    select date, symbol, price
    from meth_prices
)

{% if chain == 'mantle' %}
    , events as (
        select 
            CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', block_time) as block_timestamp
            , tx_hash_hex as tx_hash
            , index as event_index
            , 'OFTReceived' as event_name
            , '{{ chain }}' as dst_chain
            , pc_dbt_db.prod.hex_to_int(substr(data_hex, 67))::bigint as amount_received_ld
            , pc_dbt_db.prod.hex_to_int(substr(data_hex, 0, 66))::bigint as src_e_id
            , '0x' || substr(topic2_hex, 27, 40) as dst_address
            , topic1_hex as guid
            , token_messaging_address
            , stargate_implementation_pool
            , token_address
            , coingecko_id
            , decimals
            , symbol
            , 'SUCCESS' as tx_status
        from zksync_dune.{{chain}}.logs
        inner join {{ref("dim_stargate_v2_"~ chain~"_assets")}} on lower(contract_address_hex) = lower(stargate_implementation_pool)
        where topic0_hex = '0xefed6d3500546b29533b128a29e3a94d70788727f0507505ac12eaf2e578fd9c'
        {% if is_incremental() %}
            and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
    )
{% elif chain in ('berachain') %}
    , events as (
        select 
            block_timestamp
            , transaction_hash as tx_hash
            , event_index
            , 'OFTReceived' as event_name
            , '{{ chain }}' as dst_chain
            , pc_dbt_db.prod.hex_to_int(substr(data, 65))::bigint as amount_received_ld
            , pc_dbt_db.prod.hex_to_int(substr(data, 0, 64))::bigint as src_e_id
            , '0x' || substr(topics[1]::string, 27, 40) as dst_address
            , topics[0]::string as guid
            , token_messaging_address
            , stargate_implementation_pool
            , token_address
            , decimals
            , coingecko_id
            , symbol
            , 'SUCCESS' as tx_status
        from {{ref("fact_" ~ chain ~ "_events")}}
        inner join {{ref("dim_stargate_v2_"~ chain~"_assets")}} on lower(contract_address) = lower(stargate_implementation_pool)
        where topic_zero = '0xefed6d3500546b29533b128a29e3a94d70788727f0507505ac12eaf2e578fd9c'
        {% if is_incremental() %}
            and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
    )
{% else %}
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
        {% if chain in ('sei') %}
            from {{chain}}_flipside.core_evm.fact_event_logs 
        {% else %}
            from {{chain}}_flipside.core.fact_event_logs 
        {% endif %}
        inner join {{ref("dim_stargate_v2_"~ chain~"_assets")}} on lower(contract_address) = lower(stargate_implementation_pool)
        where topics [0] :: STRING = '0xefed6d3500546b29533b128a29e3a94d70788727f0507505ac12eaf2e578fd9c'
        {% if is_incremental() %}
            and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
    )
{% endif %}

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
    , coalesce(
        price, 
        case when lower(token_address) in ('0x3894085ef7ff0f0aedf52e2a2704928d1ec074f1', '0xb75d0b03c06a926e488e2659df1a861f860bd3d1') then 1 end
    ) * amount_received_ld / pow(10, events.decimals) as amount_received
    , events.tx_status
    , coalesce(
        price, 
        case when lower(token_address) in ('0x3894085ef7ff0f0aedf52e2a2704928d1ec074f1', '0xb75d0b03c06a926e488e2659df1a861f860bd3d1') then 1 end
    ) as price
from events
left join prices on block_timestamp::date = prices.date and lower(events.symbol) = lower(prices.symbol)
where events.block_timestamp < to_date(sysdate())
{% endmacro %}