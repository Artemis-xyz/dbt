{{ config(materialized="table", unique_key="date") }}
with
    base_data as (
        select raw_date as date, sum(tx_fee) as fees_native, sum(gas_usd) as fees
        from {{ ref("ez_ethereum_transactions") }}
        where
            lower(contract_address) in (
                lower('0xFf00000000000000000000000000000000008453'),
                lower('0x56315b90c40730925ec5485cf004d835058518A0')
            )
        group by raw_date
        order by raw_date desc
    )
select
    base_data.date,
    coalesce(fees_native, 0) as l1_data_cost_native,
    coalesce(fees, 0) as l1_data_cost,
    coalesce(gas, 0) - fees_native as revenue_native,
    coalesce(gas_usd, 0) - fees as revenue,
    'base' as chain
from base_data
left join
    {{ ref("agg_daily_base_fundamental_usage") }} as base on base_data.date = base.date
where base_data.date < to_date(sysdate()) and revenue is not null
