{% macro get_defillama_protocol_tvl(defillama_name) %}

with raw as (
    select 
        t.date, 
        CASE WHEN tvl = 0 THEN NULL ELSE tvl END as tvl, -- necessary to avoid 0 values from being used in forward fill
        p.name
    from pc_dbt_db.prod.fact_defillama_protocol_tvls t
    join pc_dbt_db.prod.fact_defillama_protocols p 
        on p.id = t.defillama_protocol_id 
        and p.name ilike '%{{ defillama_name }}%'
    order by t.date desc
)
, date_spine as (
    select distinct ds.date
    from {{ ref('dim_date_spine') }} ds
    where ds.date between (select min(date) from raw) and to_date(sysdate())
)
, forward_fill as (
    select 
        date_spine.date,
        COALESCE(raw.tvl, LAST_VALUE(raw.tvl IGNORE NULLS) OVER (
                PARTITION BY raw.name ORDER BY raw.date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )) as tvl,
        raw.name
    from date_spine
    left join raw
        on date_spine.date = raw.date
)
select
    date,
    tvl,
    name
from forward_fill

{% endmacro %}