{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="RAYDIUM",
    )
}}


select 
    date_trunc('day', block_timestamp) as date
    , sum(coalesce(swap_from_amount_usd, swap_to_amount_usd, 0)) as trading_volume
    , count(distinct swapper) as unique_traders
    , count(*) as number_of_swaps
from {{ ref("fact_raydium_trades") }}
where
(swap_from_amount_usd > 0 and swap_to_amount_usd > 0)
and abs(
    ln(coalesce(nullif(swap_from_amount_usd, 0), 1)) / ln(10)
    - ln(coalesce(nullif(swap_to_amount_usd, 0), 1)) / ln(10)
) < 1
{% if is_incremental() %}
    AND block_timestamp::date >= (select dateadd('day', -3, max(date)) from {{ this }})
{% else %}
    AND block_timestamp::date >= date('2022-04-22')
{% endif %}
group by 1
order by 1 desc
