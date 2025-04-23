{{ config(materialized="view") }}
with
    min_date as (
        select dateadd(day, -3, min(to_timestamp(block_timestamp))::date) date
        from gnosis_flipside.core.fact_transactions
    ),
    prices as (
        select date_trunc('day', hour) as price_date, avg(price) as price
        from ethereum_flipside.price.ez_prices_hourly
        where
            token_address = '0x6b175474e89094c44da98b954eedeac495271d0f'
            and hour >= (select min(date) from min_date)
        group by 1
    ),
    results as (
        select
            block_timestamp::date as date,
            count(distinct from_address) as daa,
            count(*) as txns,
            sum(tx_fee) gas,
            'gnosis' as chain
        from gnosis_flipside.core.fact_transactions
        group by 1
    )
select results.*, results.gas * price as gas_usd
from results
left join prices on results.date = prices.price_date
where results.date is not null
