{{ config(materialized="incremental", unique_key="date") }}
with
    base_data as (
        select raw_date as date, sum(tx_fee) as fees_native, sum(gas_usd) as fees
        from {{ ref("fact_ethereum_transactions_v2") }}
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
    coalesce(base_data.fees_native, 0) as l1_data_cost_native,
    coalesce(base_data.fees, 0) as l1_data_cost,
    'base' as chain
from base_data
where base_data.date < to_date(sysdate())
{% if is_incremental() %} 
    and base_data.date >= (
        select dateadd('day', -3, max(date))
        from {{ this }}
    )
{% endif %}
