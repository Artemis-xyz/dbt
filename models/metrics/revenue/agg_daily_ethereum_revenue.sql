{{ config(materialized="table") }}

with
    prices as (
        select date_trunc('day', hour) as price_date, avg(price) as price
        from ethereum_flipside.price.fact_hourly_token_prices
        where token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        group by 1
    ),
    chain_rev as (
        select
            date_trunc('day', block_timestamp) date,
            sum(block_burn) / 1e18 as native_token_burn
        from {{ ref("fact_ethereum_blocks") }}
        group by date
    )
select
    'ethereum' as chain,
    chain_rev.date as date,
    coalesce(native_token_burn, 0) as native_token_burn,
    coalesce(native_token_burn * price, 0) as revenue
from chain_rev
left join prices on chain_rev.date = prices.price_date
order by date desc
