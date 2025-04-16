{{ config(materialized="incremental", unique_key="date") }}
with
    expenses as (
        select raw_date as date, sum(tx_fee) as fees_native, sum(gas_usd) as fees
        from {{ ref("fact_ethereum_transactions_v2") }}
        where
            lower(contract_address)
            in (lower('0x5132A183E9F3CB7C848b0AAC5Ae0c4f0491B7aB2'))
        group by raw_date
        order by raw_date desc
    )
select
    date,
    fees_native as l1_data_cost_native,
    fees as l1_data_cost,
    'polygon_zk' as chain
from expenses
where expenses.date < to_date(sysdate())
{% if is_incremental() %} 
    and expenses.date >= (
        select dateadd('day', -3, max(date))
        from {{ this }}
    )
{% endif %}
