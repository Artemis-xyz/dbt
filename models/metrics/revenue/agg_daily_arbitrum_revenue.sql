{{ config(materialized="table", unique_key="date") }}
with
    arb_data as (
        select raw_date as date, sum(tx_fee) as fees_native, sum(gas_usd) as fees
        from {{ ref("ez_ethereum_transactions") }}
        where
            lower(contract_address) in (
                lower('0x1c479675ad559dc151f6ec7ed3fbf8cee79582b6'),
                lower('0x4c6f947Ae67F572afa4ae0730947DE7C874F95Ef')
            )
            {% if is_incremental() %}
                and raw_date >= (select dateadd('day', -7, max(date)) from {{ this }})
            {% endif %}
        group by raw_date
        order by raw_date desc
    )
select
    arb_data.date,
    coalesce(arb.fees_native, 0) - arb_data.fees_native as revenue_native,
    arb.fees - arb_data.fees as revenue,
    arb_data.fees_native as l1_data_cost_native,
    arb_data.fees as l1_data_cost,
    'arbitrum' as chain
from arb_data
left join
    {{ ref("agg_daily_arbitrum_fundamental_usage") }} as arb on arb_data.date = arb.date
where arb_data.date < to_date(sysdate()) and revenue is not null
