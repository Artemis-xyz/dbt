{{ config(materialized="table") }}

with avg_vaults_tvl as (
    select
        id,
        avg(tvl) as avg_tvl_l7d
    from {{ ref("fact_kamino_vaults_apy") }}
    where extraction_timestamp >= dateadd(day, -7, current_date)
    group by id
),

daily_avg_vaults as (
  select
    id,
    date_trunc('day', extraction_timestamp) as day,
    avg(apy) as daily_avg_apy
  from {{ ref("fact_kamino_vaults_apy") }}
  where extraction_timestamp >= dateadd(day, -7, current_date)
  group by id, date_trunc('day', extraction_timestamp)
),

with_array_vaults as (
  select
    id,
    array_agg(
      array_construct(
        date_part(epoch_second, day::timestamp_ntz),
        round(daily_avg_apy::number(38, 18), 6)
      )
    ) over (
      partition by id
      order by day
      rows between unbounded preceding and unbounded following
    ) as daily_avg_apy_l7d,
    row_number() over (partition by id order by day desc) as rn
  from daily_avg_vaults
),

l7d_vaults as (
  select distinct id, daily_avg_apy_l7d
  from with_array_vaults
  where rn = 1
),

vaults_score as (
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
        l.daily_avg_apy_l7d,
        f.extraction_timestamp
    from {{ ref("fact_kamino_vaults_apy") }} f
    left join avg_vaults_tvl a on a.id = f.id
    left join l7d_vaults l on l.id = f.id
    qualify row_number() over (partition by f.id order by extraction_timestamp desc) = 1
),

avg_lending_tvl as (
    select
        id,
        avg(tvl) as avg_tvl_l7d
    from {{ ref("fact_kamino_lending_apy") }}
    where extraction_timestamp >= dateadd(day, -7, current_date)
    group by id
),

daily_avg_lending as (
  select
    id,
    date_trunc('day', extraction_timestamp) as day,
    avg(apy) as daily_avg_apy
  from {{ ref("fact_kamino_lending_apy") }}
  where extraction_timestamp >= dateadd(day, -7, current_date)
  group by id, date_trunc('day', extraction_timestamp)
),

with_array_lending as (
  select
    id,
    array_agg(
      array_construct(
        date_part(epoch_second, day::timestamp_ntz),
        round(daily_avg_apy::number(38, 18), 6)
      )
    ) over (
      partition by id
      order by day
      rows between unbounded preceding and unbounded following
    ) as daily_avg_apy_l7d,
    row_number() over (partition by id order by day desc) as rn
  from daily_avg_lending
),

l7d_lending as (
  select distinct id, daily_avg_apy_l7d
  from with_array_lending
  where rn = 1
),

lending_score as (
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
        l.daily_avg_apy_l7d,
        f.extraction_timestamp
    from {{ ref("fact_kamino_lending_apy") }} f
    left join avg_lending_tvl a on a.id = f.id
    left join l7d_lending l on l.id = f.id
    qualify row_number() over (partition by f.id order by extraction_timestamp desc) = 1
)

select
    v.id,
    v.name,
    v.tvl_score,
    v.daily_avg_apy_l7d,
    v.extraction_timestamp
from vaults_score v
union all
select
    l.id,
    l.name,
    l.tvl_score,
    l.daily_avg_apy_l7d,
    l.extraction_timestamp
from lending_score l
