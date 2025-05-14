{{
    config(
        materialized="table",
        snowflake_warehouse="NOVA",
    )
}}

select 
    block_date::date as date,
    sum(amount_usd) as daily_volume
from {{ source("DUNE_DEX_VOLUMES", "trades")}}
where blockchain = 'nova'
group by date
order by date asc