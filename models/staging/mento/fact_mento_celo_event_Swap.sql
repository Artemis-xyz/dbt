{{config(materialized="incremental", snowflake_warehouse="MENTO", unique_key=["transaction_hash", "event_index"])}}

with
    prices as (
        {{ get_multiple_coingecko_price_with_latest('celo') }}
    )

, events as (
    select
        block_number
        , block_timestamp
        , transaction_hash
        , transaction_index
        , event_index
        , contract_address
        , decoded_log:amountIn::number as amount_in_ld
        , decoded_log:amountOut::number as amount_out_ld
        , decoded_log:exchangeId::string as exchange_id
        , decoded_log:exchangeProvider::string as exchange_provider
        , decoded_log:tokenIn::string as token_in_address
        , decoded_log:tokenOut::string as token_out_address
        , decoded_log:trader::string as trader_address
        , 'celo' as chain
    from {{ref("fact_celo_decoded_events")}}
    where event_name = 'Swap'
        and lower(contract_address) = lower('0x777A8255cA72412f0d706dc03C9D1987306B4CaD')
        {% if is_incremental() %}
            and block_timestamp >= (select DATEADD('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
    )

select
    block_number
    , block_timestamp
    , transaction_hash
    , transaction_index
    , event_index
    , events.contract_address as contract_address
    , amount_in_ld as amount_in_raw
    , amount_in_ld / power(10, prices_in.decimals) as amount_in_native
    , prices_in.price as price_in
    , prices_in.symbol as symbol_in
    , amount_in_ld / power(10, prices_in.decimals) * prices_in.price as amount_in
    
    , amount_out_ld as amount_out_raw
    , amount_out_ld / power(10, prices_out.decimals) as amount_out_native
    , prices_out.price as price_out
    , prices_out.symbol as symbol_out
    , amount_out_ld / power(10, prices_out.decimals) * prices_out.price as amount_out
    , exchange_id
    , exchange_provider
    , token_in_address
    , token_out_address
    , trader_address
    , chain
from events
left join prices prices_in on lower(events.token_in_address) = lower(prices_in.contract_address) and events.block_timestamp::date = prices_in.date
left join prices prices_out on lower(events.token_out_address) = lower(prices_out.contract_address) and events.block_timestamp::date = prices_out.date