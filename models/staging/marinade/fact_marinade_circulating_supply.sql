{{ config(
    materialized="table",
    snowflake_warehouse="MARINADE"
) }}


with token_supply as (
    select 
        date,
        circulating_supply
    from 
        {{ ref("fact_marinade_token_balances") }}
),
max_supply as (
    select
        date,
        max_by(circulating_supply, date) as circulating_supply
    from
        token_supply
    group by date
),
date_spine as (
    select
        date
    from
        pc_dbt_db.prod.dim_date_spine
    where date between (select min(date) from max_supply) and to_date(sysdate())
),
all_dates as (
    select
        ds.date,
        coalesce(last_value(tb.circulating_supply IGNORE NULLS) over (
            order by ds.date rows between unbounded preceding and current row
        ), 0) AS circulating_supply
    from
        date_spine ds
    left join
        max_supply tb on date(ds.date) = date(tb.date)
    order by
        ds.date
)
select
    date,
    circulating_supply
from
    all_dates
where date > '2021-10-05'
order by date desc
