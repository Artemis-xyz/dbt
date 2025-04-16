{{ config(materialized="table") }}
with
    gas as (
        select
            to_timestamp(block_timestamp)::date as date,
            sum(gas_used * gas_price + l1_fee) / 1e18 as gas,
            median((gas_used * gas_price + l1_fee) / 1e18) as median_gas,
            'scroll' as chain
        from {{ ref("fact_scroll_transactions") }}
        group by 1
        order by 1 asc
    ),
    prices as ({{ get_coingecko_price_with_latest("ethereum") }}),
    expenses as (
        select block_timestamp::date as date, sum(gas_used * gas_price) / 1e9 as gas
        from ethereum_flipside.core.fact_transactions
        where
            -- https://l2beat.com/scaling/projects/scroll#permissions
            lower(to_address) = lower('0xa13BAF47339d63B743e7Da8741db5456DAc1E556')
            and block_timestamp >= dateadd(day, -3, (select min(date) from gas))
        group by 1
    )
select
    gas.*,
    gas.gas * price as gas_usd,
    median_gas * price as median_gas_usd,
    (gas.gas - coalesce(expenses.gas, 0)) as revenue_native,
    (gas.gas - coalesce(expenses.gas, 0)) * price as revenue,
    expenses.gas as l1_data_cost_native,
    expenses.gas * price as l1_data_cost
from gas
left join prices on gas.date = prices.date
left join expenses on gas.date = expenses.date
where gas.date < date(sysdate())
