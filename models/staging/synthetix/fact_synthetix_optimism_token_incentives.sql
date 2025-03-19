{{ config(materialized="table", snowflake_warehouse="SYNTHETIX") }}

with optimism_token_incentives as (
    select
        date_trunc('day', block_timestamp) as date, 
        sum(coalesce(amount_usd,0)) as token_incentives
    from optimism_flipside.core.ez_token_transfers
    where from_address ILIKE '0xfeefeefeefeefeefeefeefeefeefeefeefeefeef' 
    group by date
    order by date desc
),
date_bounds as (
    select min(date) as min_date, max(date) as max_date
    from optimism_token_incentives
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
    'optimism' as chain,
    coalesce(oti.token_incentives, 0) as token_incentives
from date_series ds
left join optimism_token_incentives as oti 
    on ds.date = oti.date
order by ds.date desc
