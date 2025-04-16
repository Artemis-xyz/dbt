{{ config(materialized="incremental", unique_key="date") }}
with
    arb_data as (
        select raw_date as date, sum(tx_fee) as fees_native, sum(gas_usd) as fees
        from {{ ref("fact_ethereum_transactions_v2") }}
        where
            lower(contract_address) in (
                lower('0x1c479675ad559dc151f6ec7ed3fbf8cee79582b6'),
                lower('0x4c6f947Ae67F572afa4ae0730947DE7C874F95Ef')
            )
            {% if is_incremental() %}
                and raw_date >= (select DATEADD('day', -3, max(date)) from {{ this }})
            {% endif %}
        group by raw_date
        order by raw_date desc
    )
select
    arb_data.date,
    arb_data.fees_native as l1_data_cost_native,
    arb_data.fees as l1_data_cost,
    'arbitrum' as chain
from arb_data
where arb_data.date < to_date(sysdate())
{% if is_incremental() %} 
    and arb_data.date >= (
        select dateadd('day', -3, max(date))
        from {{ this }}
    )
{% endif %}
