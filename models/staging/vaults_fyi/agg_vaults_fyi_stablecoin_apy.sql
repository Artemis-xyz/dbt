{{ config(materialized="table") }}

with avg_tvl as (
    select
        id
        , chain
        , avg(tvl) as avg_tvl_l7d
    from {{ ref("fact_vaults_fyi_apy") }}
    where extraction_timestamp >= dateadd(day, -7, current_date)
    group by id, chain
),

daily_avg as (
  select
    id,
    chain,
    date_trunc('day', extraction_timestamp) as day,
    avg(apy) as daily_avg_apy
  from {{ ref("fact_vaults_fyi_apy") }}
  where extraction_timestamp >= dateadd(day, -7, current_date)
  group by id, chain, date_trunc('day', extraction_timestamp)
),

with_array as (
  select
    id,
    chain,
    array_agg(
      array_construct(
        date_part(epoch_second, day::timestamp_ntz),
        round(daily_avg_apy::number(38, 18), 6)
      )
    ) over (
      partition by id, chain
      order by day
      rows between unbounded preceding and unbounded following
    ) as daily_avg_apy_l7d,
    row_number() over (partition by id, chain order by day desc) as rn
  from daily_avg
),

l7d as (
  select distinct id, chain, daily_avg_apy_l7d
  from with_array
  where rn = 1
)

select
    f.id,
    f.name,
    f.chain,
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
    l.daily_avg_apy_l7d,
    f.extraction_timestamp
from {{ ref("fact_vaults_fyi_apy") }} f
left join avg_tvl a 
on a.id = f.id
and a.chain = f.chain
left join l7d l
on l.id = f.id
and l.chain = f.chain
qualify row_number() over (partition by f.id, f.chain order by extraction_timestamp desc) = 1
