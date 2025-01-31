{% macro get_defillama_protocol_tvl(defillama_name) %}

with raw as (
    select 
        t.date, 
        p.name,
        CASE WHEN t.tvl < 0 THEN 0 ELSE tvl END as tvl-- necessary to avoid 0 and negative values from being used in forward fill
    from pc_dbt_db.prod.fact_defillama_protocol_tvls t
    join pc_dbt_db.prod.fact_defillama_protocols p 
        on p.id = t.defillama_protocol_id 
        and p.name ilike '%{{ defillama_name }}%'
    order by t.date desc
)
, date_name_spine as (
    select distinct ds.date, name
    from {{ ref("dim_date_spine") }} ds
    CROSS JOIN (SELECT distinct name FROM raw)
    where ds.date between (select min(date) from raw) and to_date(sysdate())
)
, sparse as (
    select 
        ds.date,
        ds.name,
        raw.tvl
    from date_name_spine ds
    left join raw using (date, name)
)
, forward_fill as (
    SELECT
        date,
        name,
        COALESCE(tvl, LAST_VALUE(tvl IGNORE NULLS) OVER (PARTITION BY name ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) as tvl
    FROM sparse
    )
select
    date,
    '{{defillama_name}}' as name,
    sum(tvl) as tvl
from forward_fill
group by 1, 2

{% endmacro %}