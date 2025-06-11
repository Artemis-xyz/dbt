{{ config(materialized="table") }}

with aggregated_fees as (
    select
        date_trunc('day', e.block_timestamp) as date,
        e.value as bid_amount_total,
        e.value * t.price as fees_usd
    from {{source('ETHEREUM_FLIPSIDE', 'fact_transactions')}} e
    left join {{source('ETHEREUM_FLIPSIDE_PRICE', 'ez_prices_hourly')}} t
        on date_trunc('hour', e.block_timestamp) = t.hour
        and t.symbol = 'ETH' and t.name = 'ethereum'
    where e.to_address = lower('0x00C452aFFee3a17d9Cecc1Bcd2B8d5C7635C4CB9')
) 
select
    date,
    sum(bid_amount_total) as bid_amount_total,
    sum(fees_usd) as fees_usd
from aggregated_fees
group by 1