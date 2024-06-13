{{ config(snowflake_warehouse="BITCOIN") }}

with
    prices as ({{ get_coingecko_price_with_latest("bitcoin") }}),
    data as (
        select
            trunc(block_timestamp, 'day') as date,
            sum(fee) as gas,
            0 as revenue,
            'bitcoin' as chain
        from bitcoin_flipside.core.fact_transactions
        where date < to_date(sysdate())
        group by date
    )
select
    t1.date,
    t1.gas * prices.price as fees,
    t1.gas as fees_native,
    t1.revenue,
    'bitcoin' as chain
from data t1
inner join prices on t1.date = prices.date
order by date desc
