{{ config(materialized="table") }}
with
    gas as (
        select date, gas, gas_usd, 'zksync' as chain
        from {{ ref("fact_zksync_daa_txns_gas_gas_usd") }}
        where gas <> 0
    ),
    prices as (
        select date_trunc('day', hour) as price_date, avg(price) as price
        from ethereum_flipside.price.ez_prices_hourly
        where
            token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            and hour >= dateadd(day, -3, (select min(date) from gas))
        group by 1
    ),
    expenses as (
        select block_timestamp::date as date, sum(gas_used * gas_price) / 1e9 as gas
        from ethereum_flipside.core.fact_transactions
        where
            lower(to_address) in (
                lower('0x3dB52cE065f728011Ac6732222270b3F2360d919'),
                lower('0xa0425d71cB1D6fb80E65a5361a04096E0672De03'),
                lower('0xa8CB082A5a689E0d594d7da1E2d72A3D63aDc1bD'),
                lower('0x32400084C286CF3E17e7B677ea9583e60a000324')
            ) 
            and block_timestamp >= dateadd(day, -3, (select min(date) from gas))
        group by 1
    )
select gas.*, gas.gas - coalesce(expenses.gas, 0) as revenue_native, (gas.gas - coalesce(expenses.gas, 0)) * price as revenue, expenses.gas * price as l1_data_cost, expenses.gas as l1_data_cost_native
from gas
left join prices on gas.date = prices.price_date
left join expenses on gas.date = expenses.date
