{{ config(materialized="table") }}
with
    gas as (
        select
            to_timestamp(block_timestamp)::date as date,
            sum(gas_used * gas_price + l1_fee) / 1e18 as gas,
            'zora' as chain
        from {{ ref("fact_zora_transactions") }}
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
            -- https://l2beat.com/scaling/projects/zora#permissions -> sequencer and
            -- proposer from L2 beat
            lower(from_address) = lower('0x625726c858dBF78c0125436C943Bf4b4bE9d9033')
            or lower(from_address) = lower('0x48247032092e7b0ecf5dEF611ad89eaf3fC888Dd')
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
