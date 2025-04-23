{{ config(materialized="incremental", unique_key="date") }}

select
    t1.date,
    t1.chain,
    gas_usd,
    trading_fees,
    gas_usd + coalesce(trading_fees, 0) as fees,
    0 as revenue
from {{ ref("fact_osmosis_gas_gas_usd") }} t1
left join {{ ref("fact_osmosis_trading_fees") }} t2 on t1.date = t2.date
where t1.date < to_date(sysdate())
{% if is_incremental() %}
    and t1.date >= (select dateadd('day', -3, max(date)) from {{ this }})
{% endif %}
and gas_usd is not null
