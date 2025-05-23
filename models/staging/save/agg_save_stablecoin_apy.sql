{{ config(materialized="table") }}

with avg_tvl as (
    select
        id,
        avg(tvl) as avg_tvl_l7d
    from {{ ref("fact_save_apy") }}
    where extraction_timestamp >= dateadd(day, -7, current_date)
    group by id
)

select
    f.id,
    f.name,
    case 
        when a.avg_tvl_l7d >= 1e9 then 5.0
        when a.avg_tvl_l7d >= 5e8 then 4.5
        when a.avg_tvl_l7d >= 1e8 then 4.0
        when a.avg_tvl_l7d >= 5e7 then 3.5
        when a.avg_tvl_l7d >= 1e7 then 3.0
        when a.avg_tvl_l7d >= 5e6 then 2.5
        when a.avg_tvl_l7d >= 1e6 then 2.0
        when a.avg_tvl_l7d >= 5e5 then 1.5
        else 1.0
    end as tvl_score,
    f.extraction_timestamp
from {{ ref("fact_save_apy") }} f
left join avg_tvl a on a.id = f.id
qualify row_number() over (partition by f.id order by extraction_timestamp desc) = 1
