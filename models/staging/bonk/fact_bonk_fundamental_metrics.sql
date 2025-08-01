{{
    config(
        materialized='incremental',
        unique_key='date',
        snowflake_warehouse='BONK',
    )
}}
with tokens_created as (
    select 
        date_trunc('day', block_timestamp) as date,
        count(distinct mint_address) as unique_tokens_created_per_day
    from {{ ref('fact_bonk_coins_minted') }}
    group by 1
),
launchpad_metrics as (
    select 
        date
        , count(distinct swapper) as launchpad_dau
        , count(distinct tx_id) as launchpad_txns
        , sum(amount_usd) as launchpad_volume
        , count(distinct token_in) + count(distinct token_out) as unique_tokens_traded
    from {{ ref('fact_bonk_swaps') }}
    group by 1
),
bonk_fees as (
    select 
        date
        , bonk_fees as launchpad_fees
    from {{ ref('fact_bonk_fees') }}
)
select 
    coalesce(tc.date, lm.date, bf.date) as date
    , tc.unique_tokens_created_per_day
    , launchpad_dau
    , launchpad_volume
    , launchpad_txns
    , launchpad_fees
    , unique_tokens_traded
    , launchpad_volume / nullif(launchpad_txns, 0) as avg_swap_size_usd
    , launchpad_fees / nullif(launchpad_volume, 0) as fee_to_volume_ratio
from tokens_created tc
full outer join launchpad_metrics lm on tc.date = lm.date
full outer join bonk_fees bf on coalesce(tc.date, lm.date) = bf.date
{% if is_incremental() %}
where coalesce(tc.date, lm.date, bf.date) >= dateadd(day, -3, to_date(sysdate()))
{% else %}
where coalesce(tc.date, lm.date, bf.date) >= '2025-04-20'
{% endif %}
