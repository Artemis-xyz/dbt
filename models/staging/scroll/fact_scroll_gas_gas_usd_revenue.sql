{{ config(materialized="table") }}
with
    gas as (
        select
            to_timestamp(block_timestamp)::date as date,
            sum(gas_used * gas_price + l1_fee) / 1e18 as gas,
            'scroll' as chain
        from {{ ref("fact_scroll_transactions") }}
        group by 1
        order by 1 asc
    ),
    prices as (
        select date_trunc('day', hour) as price_date, avg(price) as price
        from ethereum_flipside.price.fact_hourly_token_prices
        where
            token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            and hour >= dateadd(day, -5, (select min(date) from gas))
        group by 1
    ),
    expenses as (
        select block_timestamp::date as date, sum(gas_used * gas_price) / 1e9 as gas
        from ethereum_flipside.core.fact_transactions
        where
            -- https://l2beat.com/scaling/projects/scroll#permissions
            lower(to_address) = lower('0xa13BAF47339d63B743e7Da8741db5456DAc1E556')
            and block_timestamp >= dateadd(day, -5, (select min(date) from gas))
        group by 1
    )
select
    gas.*,
    gas.gas * price as gas_usd,
    (gas.gas - coalesce(expenses.gas, 0)) * price as revenue
from gas
left join prices on gas.date = prices.price_date
left join expenses on gas.date = expenses.date
