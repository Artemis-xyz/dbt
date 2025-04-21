{{ config(materialized="table", unique_key="date") }}
with
    blast_data as (
        select raw_date as date, sum(tx_fee) as fees_native, sum(gas_usd) as fees
        from {{ ref("fact_ethereum_transactions_v2") }}
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
    'blast' as chain
from blast_data
where blast_data.date < to_date(sysdate())
{% if is_incremental() %} 
    and blast_data.date >= (
        select dateadd('day', -3, max(date))
        from {{ this }}
    )
{% endif %}
