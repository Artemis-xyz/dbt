{% macro aave_v3_reserve_factor_revenue(chain, contract_address, protocol) %}
with
revenue_events as (
    select 
        block_timestamp
        , decoded_log:reserve::string as token_address
        , decoded_log:amountMinted::float as amount
    from {{chain}}_flipside.core.ez_decoded_event_logs
    where contract_address = lower('{{contract_address}}')
        and event_name = 'MintedToTreasury'
)
, revenue_events_usd as (
    select
        block_timestamp::date as date
        , revenue_events.token_address
        , (amount / pow(10, decimals)) as amount_nominal
        , amount_nominal * price as amount_usd
    from revenue_events
    left join {{chain}}_flipside.price.ez_prices_hourly p
        on date_trunc(hour, block_timestamp) = hour 
        and lower(revenue_events.token_address) = lower(p.token_address)
)

select 
    date
    , '{{chain}}' as chain
    , '{{protocol}}' as protocol
    , token_address
    , sum(coalesce(amount_nominal, 0)) as reserve_factor_revenue_nominal
    , sum(coalesce(amount_usd, 0)) as reserve_factor_revenue_usd
from revenue_events_usd
where date < to_date(sysdate())
group by 1, 4
{% endmacro %}


{% macro aave_v2_reserve_factor_revenue(chain, contract_address, protocol) %}
with
    dim_streams as (
        select distinct
            decoded_log:streamId::number as stream_id
            , decoded_log:tokenAddress::string as token_address
        from {{chain}}_flipside.core.ez_decoded_event_logs
        where contract_address = lower('{{contract_address}}')
            and event_name = 'CreateStream'
    )
    , withdraw_from_stream_events as (
        select
            block_timestamp
            , decoded_log:streamId::number as stream_id
            , decoded_log:amount::float as amount
        from {{chain}}_flipside.core.ez_decoded_event_logs
        where contract_address = lower('{{contract_address}}')
            and event_name = 'WithdrawFromStream'
    )
    , protocol_revenue as (
        select
            withdraw_from_stream_events.block_timestamp
            , dim_streams.token_address
            , coalesce(amount / pow(10, decimals), 0) as amount_nominal
            , coalesce(amount_nominal * price, 0) as amount_usd
        from withdraw_from_stream_events
        left join dim_streams on dim_streams.stream_id = withdraw_from_stream_events.stream_id
        left join {{chain}}_flipside.price.ez_prices_hourly p
            on date_trunc(hour, block_timestamp) = hour 
            and lower(dim_streams.token_address) = lower(p.token_address)
    )
select
    block_timestamp::date as date
    , '{{chain}}' as chain
    , '{{protocol}}' as protocol
    , token_address
    , sum(coalesce(amount_nominal, 0)) as reserve_factor_revenue_nominal
    , sum(coalesce(amount_usd, 0)) as reserve_factor_revenue_usd
from protocol_revenue
where date < to_date(sysdate())
group by 1, 4
{% endmacro %}
