{{ config(materialized="table", unique_key="date") }}
with
    expenses as (
        select raw_date as date, sum(tx_fee) as fees_native, sum(gas_usd) as fees
        from {{ ref("ez_ethereum_transactions") }}
        where
            lower(contract_address)
            in (lower('0x5132A183E9F3CB7C848b0AAC5Ae0c4f0491B7aB2'))
        group by raw_date
        order by raw_date desc
    )
select
    top_line.date,
    expenses.fees_native as expenses_native,
    expenses.fees as expenses,
    coalesce(top_line.gas_usd, 0) - expenses.fees as revenue,
    'polygon_zk' as chain
from {{ ref("fact_polygon_zk_daa_txns_gas_usd") }} as top_line
left join expenses on expenses.date = top_line.date
where expenses.date < to_date(sysdate()) and revenue is not null
