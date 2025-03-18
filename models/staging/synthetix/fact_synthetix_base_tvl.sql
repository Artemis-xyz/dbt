{{ config(materialized="table", snowflake_warehouse="SYNTHETIX") }}

with recursive
  partitioned_transactions as (
    select 
      date_trunc('hour', block_timestamp) as hourly_timestamp,
      contract_address,
      balance_token,
      row_number() over (
        partition by date_trunc('day', block_timestamp), contract_address
        order by block_timestamp desc
      ) as rn
    from base.prod_raw.ez_address_balances_by_token
    where address ILIKE '0x32C222A9A159782aFD7529c87FA34b96CA72C696'
  ),
  daily_tvl as (
    select 
      date_trunc('day', hourly_timestamp) as date,
      sum(coalesce(balance_token/pow(10,coalesce(decimals,0)), 0) * eph.price) as tvl_usd
    from partitioned_transactions pt
    inner join base_flipside.price.ez_prices_hourly eph
      on pt.contract_address = eph.token_address
     and pt.hourly_timestamp = eph.hour
    where rn = 1
    group by 1
  ),
  date_bounds as (
    select min(date) as min_date, max(date) as max_date
    from daily_tvl
  ),
  date_series as (
    -- base case: start at the minimum date
    select min_date as date
    from date_bounds
    union all
    -- recursive step: add one day until reaching the maximum date
    select date + interval '1 day'
    from date_series, date_bounds
    where date < max_date
  ),
  daily_with_series as (
    select ds.date, dt.tvl_usd
    from date_series ds
    left join daily_tvl dt on ds.date = dt.date
  )
select 
  date,
  /* this window function carries forward the last non-null tvl_usd */
  last_value(tvl_usd ignore nulls) over (
    order by date rows between unbounded preceding and current row
  ) as tvl_usd
from daily_with_series
order by date desc