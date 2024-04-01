{{ config(materialized="table") }}
with
    prices as (
        select date_trunc('day', hour) as price_date, avg(price) as price
        from ethereum_flipside.price.fact_hourly_token_prices
        where token_address = '0x6b175474e89094c44da98b954eedeac495271d0f'
        group by 1
    ),
    chain_rev as (
        select
            date_trunc('day', block_timestamp) date,
            sum(block_burn) / 1e18 as native_token_burn
        from {{ ref("fact_gnosis_blocks") }}
        group by date
    )
select
    'gnosis' as chain,
    chain_rev.date as date,
    coalesce(native_token_burn, 0) as native_token_burn,
    coalesce(native_token_burn * price, 0) as revenue
from chain_rev
left join prices on chain_rev.date = prices.price_date
where chain_rev.date is not null
order by date desc
