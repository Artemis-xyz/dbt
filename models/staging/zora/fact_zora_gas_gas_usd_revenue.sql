{{ config(materialized="table") }}
with
    gas as (
        select
            to_timestamp(block_timestamp)::date as date,
            sum(gas_used * gas_price + l1_fee) / 1e18 as gas,
            median(gas_used * gas_price + l1_fee) as median_gas,
            'zora' as chain
        from {{ ref("fact_zora_transactions") }}
        group by 1
        order by 1 asc
    ),
    prices as ({{ get_coingecko_price_with_latest("ethereum") }}),
    expenses as (
        select block_timestamp::date as date, sum(gas_used * gas_price) / 1e9 as gas
        from ethereum_flipside.core.fact_transactions
        where
            -- https://l2beat.com/scaling/projects/zora#permissions -> sequencer and
            -- proposer from L2 beat
            lower(from_address) = lower('0x625726c858dBF78c0125436C943Bf4b4bE9d9033')
            or lower(from_address) = lower('0x48247032092e7b0ecf5dEF611ad89eaf3fC888Dd')
            and block_timestamp >= dateadd(day, -3, (select min(date) from gas))
        group by 1
    )
select
    gas.*,
    gas.gas * price as gas_usd,
    median_gas * price as median_gas_usd,
    (gas.gas - coalesce(expenses.gas, 0)) * price as revenue,
    (gas.gas - coalesce(expenses.gas, 0)) as revenue_native,
    expenses.gas as l1_data_cost_native,
    expenses.gas * price as l1_data_cost
from gas
left join prices on gas.date = prices.date
left join expenses on gas.date = expenses.date
where gas.date < date(sysdate())
