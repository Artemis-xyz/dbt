{{ config(materialized="incremental", unique_key="date") }}
with
    starknet_data as (
        select raw_date as date, sum(tx_fee) as fees_native, sum(gas_usd) as fees
        from {{ ref("ez_ethereum_transactions") }}
        where
            lower(contract_address) in (
                lower('0xc662c410C0ECf747543f5bA90660f6ABeBD9C8c4'),
                lower('0x47312450B3Ac8b5b8e247a6bB6d523e7605bDb60')
            )
            {% if is_incremental() %}
                and raw_date >= (select dateadd('day', -7, max(date)) from {{ this }})
            {% endif %}
        group by raw_date
        order by raw_date desc
    )
select
    starknet_data.date,
    starknet_data.fees_native as l1_data_cost_native,
    starknet_data.fees as l1_data_cost,
    'starknet' as chain
from starknet_data
where starknet_data.date < to_date(sysdate())
{% if is_incremental() %} 
    and starknet_data.date >= (
        select dateadd('day', -5, max(date))
        from {{ this }}
    )
{% endif %}
