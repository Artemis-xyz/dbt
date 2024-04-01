select
    t1.date,
    t1.chain,
    gas,
    gas_usd,
    trading_fees,
    gas_usd + trading_fees as fees,
    0 as revenue
from {{ ref("fact_osmosis_gas_gas_usd") }} t1
left join {{ ref("fact_osmosis_trading_fees") }} t2 on t1.date = t2.date
where t1.date < to_date(sysdate())
