
{{
    config(
        materialized='table',
        snowflake_warehouse='METEORA',
        database='METEORA',
        schema='core',
        alias='ez_metrics',
    )
 }}

with date_spine as (
    select date_spine.date
    from {{ ref('dim_date_spine') }} date_spine
    where date_spine.date between '2023-10-01' and to_date(sysdate())
)

, swap_metrics as (
    SELECT
        block_timestamp::date as date,
        count(distinct swapper) as unique_traders, 
        count(distinct tx_id) as number_of_swaps,
        sum(swap_from_amount_usd) as amount_in_usd,
        sum(swap_to_amount_usd) as amount_out_usd,
        count(
            distinct concat(swap_to_symbol,'-',swap_from_symbol)
        ) as pairs_traded,
        sum(coalesce(swap_to_amount_usd, swap_from_amount_usd)) as trading_volume 
    FROM {{ source('SOLANA_FLIPSIDE_DEFI', 'ez_dex_swaps') }}
    WHERE swap_program ilike '%meteora%'
    group by 1
)

select
    date_spine.date,
    coalesce(swap_metrics.unique_traders, 0) as unique_swappers,
    coalesce(swap_metrics.number_of_swaps, 0) as number_of_swaps,
    coalesce(swap_metrics.trading_volume, 0) as trading_volume
from date_spine
left join swap_metrics using(date)