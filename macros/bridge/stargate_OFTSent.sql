{% macro stargate_OFTSent(chain)%}
with 
{% if chain in ('berachain', 'mantle') %}
    prices as (
        select date, coingecko_id, shifted_token_price_usd as price
        from {{ ref("fact_coingecko_token_date_adjusted_gold") }}
        where date < dateadd(day, -1, to_date(sysdate()))
        union
        select 
            dateadd('day', -1, to_date(sysdate())) as date,
            token_id as coingecko_id,
            token_current_price as price
        from {{ ref("fact_coingecko_token_realtime_data") }}    
    )
{% else %}
    prices as (
        select
            hour::date as date
            , token_address as contract_address
            , avg(price) as price
        from {{chain}}_flipside.price.ez_prices_hourly
        group by 1, 2
        --On 2025-02-12, SEI Flipside does not contain prices for the eth token. Need to add this manually.
        {% if chain == 'sei' %}
            union all
                select
                    date
                    , '0x160345fc359604fc6e70e3c5facbde5f7a9342d8' as contract_address
                    , price
                from ({{ get_coingecko_price_with_latest('ethereum')}})
                where not exists (
                    select 1 
                    from {{chain}}_flipside.price.ez_prices_hourly 
                    where lower(token_address) = lower('0x160345fc359604fc6e70e3c5facbde5f7a9342d8')
                    limit 1
                )
        {% endif %}
    )
{% endif %}

{% if chain == 'mantle' %}
    , events as (
        select 
            CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', block_time) as block_timestamp
            , tx_hash_hex as tx_hash
            , index as event_index
            , 'OFTSent' as event_name
            , '{{ chain }}' as src_chain
            , pc_dbt_db.prod.hex_to_int(substr(data_hex, 67, 64))::bigint as amount_sent_ld
            , pc_dbt_db.prod.hex_to_int(substr(data_hex, 0, 66))::bigint as dst_e_id
            , '0x' || substr(topic2_hex, 27, 40) as src_address
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
        where topic0_hex = '0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a' 
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
            , 'OFTSent' as event_name
            , '{{ chain }}' as src_chain
            , pc_dbt_db.prod.hex_to_int(substr(data, 67, 64))::bigint as amount_sent_ld
            , pc_dbt_db.prod.hex_to_int(substr(data, 0, 66))::bigint as dst_e_id
            , '0x' || substr(topics[1]::string, 27, 40) as src_address
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
        where topic_zero = '0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a' 
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
        , 'OFTSent' as event_name
        , '{{ chain }}' as src_chain
        , pc_dbt_db.prod.hex_to_int(substr(data, 67, 64))::bigint as amount_sent_ld
        , pc_dbt_db.prod.hex_to_int(substr(data, 0, 66))::bigint as dst_e_id
        , '0x' || substr(topics[2]::string, 27, 40) as src_address
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
    where topics [0] :: STRING = '0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a' 
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
{% if chain in ('berachain', 'mantle') %}
    left join prices on block_timestamp::date = prices.date and lower(events.coingecko_id) = lower(prices.coingecko_id)
{% else %}
    left join prices on block_timestamp::date = prices.date and lower(prices.contract_address) = lower(token_address)
{% endif %}
{% endmacro %}