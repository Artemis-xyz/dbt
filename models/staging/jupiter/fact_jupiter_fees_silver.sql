{{
    config(
        materialized="table",
        unique_key="date",
        snowflake_warehouse="JUPITER",
    )
}}

select
   coalesce(lo.date, dca.date, perps.date) as date,
   coalesce(dca.fees,0) + coalesce(lo.fees,0) + coalesce(perps.fees, 0) as fees
from
    {{ ref("fact_jupiter_limit_order_fees_silver") }} lo
full join
     {{ ref("fact_jupiter_dca_fees_silver") }} dca on dca.date = lo.date
full join
    {{ ref("fact_jupiter_perps_silver") }} perps on perps.date = dca.date
where coalesce(lo.date, dca.date, perps.date) < to_date(sysdate())
order by 1 asc