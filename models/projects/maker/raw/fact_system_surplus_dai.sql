{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_system_surplus_dai"
    )
}}

with bs_equity as (
    select
        date(ts) as date,
        sum(case when acc.code not like '33%' and acc.code not like '39%' then acc.value else 0 end) as surplus
    from {{ ref('fact_final') }} acc
    where code like '3%'
    and acc.code not like '33%' and acc.code not like '39%'
    group by 1
)
select
    date,
    sum(surplus) over (order by date) as surplus,
    'DAI' as token
from bs_equity