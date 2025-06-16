{{ config(materialized="table") }}

with avg_tvl as (
    select
        name,
        avg(tvl) as avg_tvl_l7d
    from {{ ref("fact_susdf_apy") }}
    where extraction_timestamp >= dateadd(day, -7, current_date)
    group by name
),

l7d as (
  select
    name,
    ARRAY_AGG(
      ARRAY_CONSTRUCT(
        DATE_PART(EPOCH_SECOND, extraction_timestamp::TIMESTAMP_NTZ),
        round(apy * 100, 6)
      )
    ) WITHIN GROUP (ORDER BY extraction_timestamp ASC) AS daily_avg_apy_l7d
  from {{ ref("fact_susdf_apy") }}
  where extraction_timestamp >= dateadd(day, -7, current_date)
  group by name
)

select
    l.name,
    case 
        when avg_tvl_l7d >= 1e9 then 5.0
        when avg_tvl_l7d >= 5e8 then 4.5
        when avg_tvl_l7d >= 1e8 then 4.0
        when avg_tvl_l7d >= 5e7 then 3.5
        when avg_tvl_l7d >= 1e7 then 3.0
        when avg_tvl_l7d >= 5e6 then 2.5
        when avg_tvl_l7d >= 1e6 then 2.0
        when avg_tvl_l7d >= 5e5 then 1.5
        else 1.0
    end as tvl_score,
    l.daily_avg_apy_l7d,
    f.extraction_timestamp
from {{ ref("fact_susdf_apy") }} f
left join avg_tvl on avg_tvl.name = f.name
left join l7d l on l.name = f.name
qualify row_number() over (order by extraction_timestamp desc) = 1
