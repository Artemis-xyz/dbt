{{
    config(
        materialized="incremental",
        snowflake_warehouse="SONIC",
        unique_key=['tx_hash', 'event_index'],
    )
}}

with prices as (
    {{ get_multiple_coingecko_price_with_latest('ethereum') }}
)
select 
    block_timestamp
    , date_trunc('day', block_timestamp) as date
    , logs.contract_address as src_messaging_contract_address
    , tx_hash
    , event_index
    , case when event_name = 'Claim' then 'sonic' else 'ethereum' end as source_chain         
    , case when event_name = 'Claim' then 'ethereum' else 'sonic' end as destination_chain
    , decimals
    , symbol
    , case when contains(lower(symbol), 'usd') then 'Stablecoin' else 'Token' end as category
    , decoded_log:"token"::string as token_address
    , decoded_log:"amount"::number as amount_native
    , decoded_log:"amount"::number / POW(10, decimals) as amount_adjusted
    , decoded_log:"amount"::number / POW(10, decimals) * prices.price as amount
from ethereum_flipside.core.ez_decoded_event_logs as logs
left join prices on lower(prices.contract_address) = lower(logs.decoded_log:"token"::string) and prices.date = date_trunc('day', logs.block_timestamp)
where lower(logs.contract_address) = lower('0xa1E2481a9CD0Cb0447EeB1cbc26F1b3fff3bec20') and event_name in ('Deposit', 'Claim')
{% if is_incremental() %}
    and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
{% else %}
    and block_timestamp >= '2024-12-20' 
{% endif %}
