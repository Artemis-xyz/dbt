{{
    config(
        materialized="table",
        snowflake_warehouse="POLYGON_ZK",
    )
}}

select 
    block_date::date as date,
    sum(amount_usd) as daily_volume
from {{ source("DUNE_DEX_VOLUMES", "trades")}}
where blockchain = 'zkevm'
group by date
order by date asc