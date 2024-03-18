{{ config(materialized="table") }}

with
    prices as (
        select date as price_date, shifted_token_price_usd as price
        from {{ ref("fact_coingecko_token_date_adjusted_gold") }}
        where coingecko_id = 'near'
        union
        select
            dateadd('day', -1, to_date(sysdate())) as price_date,
            token_current_price as price
        from {{ ref("fact_coingecko_token_realtime_data") }}
        where token_id = 'near'
    ),
    max_prices as (
        select price_date, max(price) as price from prices group by price_date
    ),
    native_revs as (
        select
            date_trunc('day', block_timestamp) as date,
            sum(value:"balance_burnt"::number) / pow(10, 24) as revenue_native
        from near_flipside.core.fact_blocks, lateral flatten(chunks)
        group by date
    )
select
    native_revs.date,
    native_revs.revenue_native,
    revenue_native * coalesce(price, 0) as revenue,
    'near' as chain
from native_revs
left join max_prices on native_revs.date = max_prices.price_date
order by date desc
