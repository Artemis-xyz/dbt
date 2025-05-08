{{
    config(
        materialized="table",
        snowflake_warehouse="BASE",
    )
}}

select 
    block_date::date as date,
    sum(amount_usd) as daily_volume
from {{ source("DUNE_DEX_VOLUMES", "trades") }}
where blockchain = 'base'
  and not (
    (date = '2025-04-25' and tx_from in (
        TO_BINARY('80205B17056BE5F99A9EB028E811C20DDF896F2A', 'HEX'), 
        TO_BINARY('E3223F7E3343C2C8079F261D59EE1E513086C7C3', 'HEX')
    ))
    or
    (date = '2025-04-29' and tx_from in (
        TO_BINARY('80205B17056BE5F99A9EB028E811C20DDF896F2A', 'HEX'), 
        TO_BINARY('E3223F7E3343C2C8079F261D59EE1E513086C7C3', 'HEX')
    ))
)
group by date
order by date asc

