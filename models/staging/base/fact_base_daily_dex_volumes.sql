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
    (date = '2025-04-25' and lower(tx_from_hex) in (
        lower('0x80205B17056BE5F99A9EB028E811C20DDF896F2A'), 
        lower('0xE3223F7E3343C2C8079F261D59EE1E513086C7C3')
    ))
    or
    (date = '2025-04-29' and lower(tx_from_hex) in (
        lower('0x80205B17056BE5F99A9EB028E811C20DDF896F2A'), 
        lower('0xE3223F7E3343C2C8079F261D59EE1E513086C7C3')
    ))
    or
    (date = '2025-05-07' and lower(tx_from_hex) in (
        lower('0xf14149bde6f7e2573f38aceda6220d7dfff66592')
    ))
)
group by date
order by date asc

