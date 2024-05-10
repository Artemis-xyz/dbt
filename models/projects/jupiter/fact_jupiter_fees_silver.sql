select
   lo.date,
   coalesce(dca.fees,0) + coalesce(lo.fees,0) + coalesce(perps.fees, 0) as fees
from
    pc_dbt_db.prod.fact_jupiter_limit_order_fees_silver lo
left join
     pc_dbt_db.prod.fact_jupiter_dca_fees_silver dca on dca.date = lo.date
left join
    pc_dbt_db.prod.fact_jupiter_perps_silver perps on perps.date = dca.date
where lo.date < to_date(sysdate())
order by 1 asc