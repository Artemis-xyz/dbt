{{ config(materialized="table") }}

with
    distinct_tokens as (
        select distinct token_address
        from {{ ref("fact_arbitrum_one_bridge_transfers") }}
    ),

    prices as (
        select *
        from ethereum_flipside.price.ez_hourly_token_prices
        where token_address in (select * from distinct_tokens)
        union
        select *
        from arbitrum_flipside.price.ez_hourly_token_prices
        where token_address in (select * from distinct_tokens)
    ),

    hourly_volume as (
        select
            date_trunc('hour', block_timestamp) as hour,
            source_chain,
            destination_chain,
            t.token_address,
            sum(
                (coalesce(amount::bigint, 0) / power(10, coalesce(p.decimals, 18)))
                * coalesce(price, 0)
            ) as amount_usd
        from {{ ref("fact_arbitrum_one_bridge_transfers") }} t
        left join
            prices p
            on date_trunc('hour', t.block_timestamp) = p.hour
            and t.token_address = p.token_address
        where p.symbol != 'ShibDoge'
        group by 1, 2, 3, 4
    )

select
    date_trunc('day', hour) as date,
    'arbitrum_one_bridge' as app,
    source_chain,
    destination_chain,
    category,
    sum(amount_usd) as amount_usd,
    null as fee_usd
from hourly_volume
left join {{ ref("dim_contracts_gold") }} t2 on lower(token_address) = lower(t2.address)
group by 1, 2, 3, 4, 5
