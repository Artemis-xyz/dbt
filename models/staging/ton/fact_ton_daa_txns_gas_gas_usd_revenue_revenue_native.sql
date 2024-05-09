select 
    t1.date,
    t1.chain,
    daa as dau,
    txns,
    gas as fees_native,
    gas_usd as fees,
    revenue_native,
    revenue,
    fees / txns as avg_txn_fee,
from {{ ref("fact_ton_txns")}} t1
left join {{ ref("fact_ton_daa")}} t2 using(date)
left join {{ ref("fact_ton_gas_gas_usd")}} t3 using(date)
left join {{ ref("fact_ton_revenue_revenue_native")}} t4 using(date)


    