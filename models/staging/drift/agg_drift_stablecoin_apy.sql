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
    avg(apy) * 100 as daily_avg_apy
  from {{ ref("fact_drift_insurance_vault_apy") }}
  where extraction_timestamp >= dateadd(day, -7, current_date)
  group by market, date_trunc('day', extraction_timestamp)
),

l7d_if as (
  select
    market,
    ARRAY_AGG(
      ARRAY_CONSTRUCT(
        DATE_PART(EPOCH_SECOND, day::TIMESTAMP_NTZ),
        ROUND(daily_avg_apy::NUMBER(38, 18), 6)
      )
    ) WITHIN GROUP (ORDER BY day ASC) AS daily_avg_apy_l7d
  from daily_avg_if
  group by market
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
    avg(apy) * 100 as daily_avg_apy
  from {{ ref("fact_drift_lending_apy") }}
  where extraction_timestamp >= dateadd(day, -7, current_date)
  group by market, date_trunc('day', extraction_timestamp)
),

l7d_lending as (
  select
    market,
    ARRAY_AGG(
      ARRAY_CONSTRUCT(
        DATE_PART(EPOCH_SECOND, day::TIMESTAMP_NTZ),
        ROUND(daily_avg_apy::NUMBER(38, 18), 6)
      )
    ) WITHIN GROUP (ORDER BY day ASC) AS daily_avg_apy_l7d
  from daily_avg_lending
  group by market
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