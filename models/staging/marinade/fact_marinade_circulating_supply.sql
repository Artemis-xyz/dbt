{{ config(
    materialized="incremental",
    snowflake_warehouse="MARINADE"
) }}


with token_supply as (
    select 
        date(block_timestamp) as date,
        1000000000 - balance as remaining_balance
    from 
        solana_flipside.core.fact_token_balances
    where 
        lower(account_address) = lower('GR1LBT4cU89cJWE74CP6BsJTf2kriQ9TX59tbDsfxgSi')
        {% if is_incremental() %}
            and date(block_timestamp) >= (select dateadd('day', -3, max(date)) from {{ this }})
        {% endif %}
    order by
        date(block_timestamp) desc
),
max_supply as (
    select
        date,
        max_by(remaining_balance, date) as remaining_balance
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
        coalesce(last_value(tb.remaining_balance IGNORE NULLS) over (
            order by ds.date rows between unbounded preceding and current row
        ), 0) AS remaining_balance
    from
        date_spine ds
    left join
        max_supply tb on date(ds.date) = date(tb.date)
    order by
        ds.date
)
select
    date,
    remaining_balance
from
    all_dates
where date > '2021-10-05'
order by date desc
