{{ config(materialized="table", unique_key="date") }}
with
    opt_data as (
        select raw_date as date, sum(tx_fee) as fees_native, sum(gas_usd) as fees
        from {{ ref("fact_ethereum_transactions_gold") }}
        where
            lower(contract_address) in (
                lower('0x4BF681894abEc828B212C906082B444Ceb2f6cf6'),
                lower('0x5E4e65926BA27467555EB562121fac00D24E9dD2'),
                lower('0xFF00000000000000000000000000000000000010'),
                lower('0xbe5dab4a2e9cd0f27300db4ab94bee3a233aeb19'),
                lower('0xd2e67b6a032f0a9b1f569e63ad6c38f7342c2e00'),
                lower('0xe969c2724d2448f1d1a6189d3e2aa1f37d5998c1')
            )
        group by raw_date
        order by raw_date desc
    )
select
    opt_data.date,
    fees_native as l1_data_cost_native,
    fees as l1_data_cost,
    coalesce(gas, 0) - fees_native as revenue_native,
    coalesce(gas_usd, 0) - fees as revenue,
    'optimism' as chain
from opt_data
left join
    {{ ref("agg_daily_optimism_fundamental_usage") }} as opt on opt_data.date = opt.date
where opt_data.date < to_date(sysdate()) and revenue is not null
