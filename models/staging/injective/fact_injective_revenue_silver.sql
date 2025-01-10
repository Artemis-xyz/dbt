select r.date, r.revenue_native * p.price as revenue, r.revenue_native as revenue_native, 'injective' as chain
from {{ ref("fact_injective_revenue_native_silver")}} r
left join
    ({{ get_coingecko_price_with_latest("injective-protocol") }}) p on p.date = r.date
where r.date < to_date(sysdate())
order by r.date desc
