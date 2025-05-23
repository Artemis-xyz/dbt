{{ config(materialized="table") }}

with avg_if_tvl as (
    select
        market,
        avg(tvl) as avg_tvl_l7d
    from {{ ref("fact_drift_insurance_vault_apy") }}
    where extraction_timestamp >= dateadd(day, -7, current_date)
    group by market
),

daily_avg_if as (
  select
    market,
    date_trunc('day', extraction_timestamp) as day,
    avg(apy) as daily_avg_apy
  from {{ ref("fact_drift_insurance_vault_apy") }}
  where extraction_timestamp >= dateadd(day, -7, current_date)
  group by market, date_trunc('day', extraction_timestamp)
),

with_array_if as (
  select
    market,
    array_agg(
      array_construct(
        date_part(epoch_second, day::timestamp_ntz),
        round(daily_avg_apy::number(38, 18), 6)
      )
    ) over (
      partition by market
      order by day
      rows between unbounded preceding and unbounded following
    ) as daily_avg_apy_l7d,
    row_number() over (partition by market order by day desc) as rn
  from daily_avg_if
),

l7d_if as (
  select distinct market, daily_avg_apy_l7d
  from with_array_if
  where rn = 1
),

if_score as (
    select
        f.market,
        f.type,
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
    from {{ ref("fact_drift_insurance_vault_apy") }} f
    left join avg_if_tvl a on a.market = f.market
    left join l7d_if l on l.market = f.market
    qualify row_number() over (partition by f.market order by extraction_timestamp desc) = 1
),

avg_lending_tvl as (
    select
        market,
        avg(tvl) as avg_tvl_l7d
    from {{ ref("fact_drift_lending_apy") }}
    where extraction_timestamp >= dateadd(day, -7, current_date)
    group by market
),

daily_avg_lending as (
  select
    market,
    date_trunc('day', extraction_timestamp) as day,
    avg(apy) as daily_avg_apy
  from {{ ref("fact_drift_lending_apy") }}
  where extraction_timestamp >= dateadd(day, -7, current_date)
  group by market, date_trunc('day', extraction_timestamp)
),

with_array_lending as (
  select
    market,
    array_agg(
      array_construct(
        date_part(epoch_second, day::timestamp_ntz),
        round(daily_avg_apy::number(38, 18), 6)
      )
    ) over (
      partition by market
      order by day
      rows between unbounded preceding and unbounded following
    ) as daily_avg_apy_l7d,
    row_number() over (partition by market order by day desc) as rn
  from daily_avg_lending
),

l7d_lending as (
  select distinct market, daily_avg_apy_l7d
  from with_array_lending
  where rn = 1
),

lending_score as (
    select
        f.market,
        f.type,
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
    from {{ ref("fact_drift_lending_apy") }} f
    left join avg_lending_tvl a on a.market = f.market
    left join l7d_lending l on l.market = f.market
    qualify row_number() over (partition by f.market order by extraction_timestamp desc) = 1
)

select
    i.market,
    i.type,
    i.tvl_score,
    i.daily_avg_apy_l7d,
    i.extraction_timestamp
from if_score i
union all
select
    l.market,
    l.type,
    l.tvl_score,
    l.daily_avg_apy_l7d,
    l.extraction_timestamp
from lending_score l