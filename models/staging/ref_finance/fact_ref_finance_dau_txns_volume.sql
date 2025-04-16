{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'REF_FINANCE',
        unique_key = ['date']
    )
}}

select
    date(block_timestamp) as date,
    count(distinct(concat(tx_hash, swap_index))) as daily_swaps,
    count(distinct(trader)) as unique_traders,
    SUM(CASE 
        WHEN amount_out_usd >= amount_in_usd * 10 THEN amount_in_usd
        ELSE amount_out_usd
    END) AS volume
from {{ source('NEAR_FLIPSIDE', 'ez_dex_swaps') }}
group by 1
