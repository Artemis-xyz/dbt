select r.date, r.revenue_native * p.price as revenue, 'injective' as chain
from pc_dbt_db.prod.fact_injective_revenue_native_silver r
left join
    ({{ get_coingecko_price_with_latest("injective-protocol") }}) p on p.date = r.date
where r.date < to_date(sysdate())
order by r.date desc
