{{ config(materialized="table", snowflake_warehouse="SYNTHETIX") }}

with base_daily as (
    select 
        date_trunc('day', block_timestamp) as date, 
        sum(coalesce(amount_usd, 0)) as daily_expenses
    from ethereum_flipside.core.ez_token_transfers
    where from_address ilike '0x99f4176ee457afedffcb1839c7ab7a030a5e4a92'
      and to_address not ilike '0x9008d19f58aabd9ed0d60971565aa8510560ab41'
    group by date
),
date_bounds as (
    select min(date) as min_date, max(date) as max_date
    from base_daily
),
date_series as (
    select min_date as date
    from date_bounds
    union all
    select date + interval '1 day'
    from date_series, date_bounds
    where date < max_date
)
select 
    ds.date,
    coalesce(bd.daily_expenses, 0) as daily_expenses
from date_series ds
left join base_daily bd on ds.date = bd.date
order by ds.date desc
