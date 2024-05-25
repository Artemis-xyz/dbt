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
    coalesce(blast_data.fees_native, 0) as l1_data_cost_native,
    coalesce(blast_data.fees, 0) as l1_data_cost,
    coalesce(blast.fees_native, 0) - blast_data.fees_native as revenue_native,
    coalesce(blast.fees, 0) - blast_data.fees as revenue,
    'blast' as chain
from blast_data
left join
    {{ ref("agg_daily_blast_fundamental_usage") }} as blast on blast_data.date = blast.date
where blast_data.date < to_date(sysdate()) and revenue is not null
