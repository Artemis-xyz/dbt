{{
    config(
        materialized="incremental",
        snowflake_warehouse= "STABLECOIN_DAILY",
    )
}}

select
    b.date,
    b.symbol,
    b.app,
    b.category,
    l.subregion,
    sum(b.stablecoin_supply) as stablecoin_supply,
from pc_dbt_db.prod.dim_geo_labels l
left join {{ ref("agg_daily_stablecoin_breakdown_silver") }} b
on lower(b.from_address) = lower(l.address)
and b.chain = l.chain
where l.subregion is not null
{% if is_incremental() %}
    and b.date >= (select dateadd('day', -7, max(date)) from {{ this }})
{% endif %}
group by 1,2,3,4,5