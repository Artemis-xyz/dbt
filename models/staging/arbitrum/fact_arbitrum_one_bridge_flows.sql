{{ config(materialized="table") }}

with
    distinct_tokens as (
        select distinct token_address
        from {{ ref("fact_arbitrum_one_bridge_transfers") }}
    ),

    prices as (
        select 
             hour
            , token_address
            , decimals
            , price
            , symbol
        from ethereum_flipside.price.ez_prices_hourly
        where token_address in (select * from distinct_tokens)
        union
        select 
             hour
            , token_address
            , decimals
            , price
            , symbol
        from arbitrum_flipside.price.ez_prices_hourly
        where token_address in (select * from distinct_tokens)
    ),

    dim_contracts as (
        select distinct address, chain, category
        from {{ ref("dim_contracts_gold") }} 
        where category is not null and chain is not null
    ),

    hourly_volume as (
        select
            date_trunc('hour', block_timestamp) as hour,
            source_chain,
            destination_chain,
            coalesce(c.category, 'Not Categorized') as category,
            p.symbol,
            coalesce(amount::bigint / power(10, p.decimals) * price, 0) as amount_usd
        from {{ ref("fact_arbitrum_one_bridge_transfers") }} t
        left join
            prices p
            on date_trunc('hour', t.block_timestamp) = p.hour
            and t.token_address = p.token_address
        left join dim_contracts c on lower(t.token_address) = lower(c.address) and c.chain = 'ethereum' --only using l1token
        where p.symbol != 'ShibDoge'
    )

select
    date_trunc('day', hour) as date,
    'arbitrum' as app,
    source_chain,
    destination_chain,
    category,
    symbol,
    sum(amount_usd) as amount_usd,
    null as fee_usd
from hourly_volume
group by 1, 2, 3, 4, 5, 6
