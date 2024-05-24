{{ config(materialized="table", unique_key="date") }}
with
    blast_data as (
        select raw_date as date, sum(tx_fee) as fees_native, sum(gas_usd) as fees
        from {{ ref("ez_ethereum_transactions") }}
        where
            lower(contract_address) in (
                lower('0xFf00000000000000000000000000000000081457')
            )
        group by raw_date
        order by raw_date desc
    )
select
    blast_data.date,
    coalesce(fees_native, 0) as l1_data_cost_native,
    coalesce(fees, 0) as l1_data_cost,
    coalesce(gas, 0) - fees_native as revenue_native,
    coalesce(gas_usd, 0) - fees as revenue,
    'blast' as chain
from blast_data
left join
    {{ ref("agg_daily_blast_fundamental_usage") }} as blast on blast_data.date = blast.date
where blast_data.date < to_date(sysdate()) and revenue is not null
