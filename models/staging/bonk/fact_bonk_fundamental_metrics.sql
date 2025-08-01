{{
    config(
        materialized='incremental',
        unique_key='date',
        snowflake_warehouse='BONK',
    )
}}
-- Still need to add fees. 
with coins_minted as (
    select 
        date_trunc('day', block_timestamp) as date,
        count (distinct mint_address) as coins_minted
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
),
select 
    coalesce(cm.date, lm.date, bf.date, tsm.date) as date
    , coins_minted
    , launchpad_dau
    , launchpad_volume
    , launchpad_txns
    , launchpad_fees
    , unique_tokens_traded
    , launchpad_volume / nullif(launchpad_txns, 0) as avg_swap_size_usd
    , launchpad_fees / nullif(launchpad_volume, 0) as fee_to_volume_ratio
    , total_token_supply
    , avg_token_supply
    , unique_symbols_created
from coins_minted cm
full outer join launchpad_metrics lm on cm.date = lm.date
full outer join bonk_fees bf on coalesce(cm.date, lm.date) = bf.date
full outer join token_supply_metrics tsm on coalesce(cm.date, lm.date, bf.date) = tsm.date
{% if is_incremental() %}
where coalesce(cm.date, lm.date, bf.date, tsm.date) >= dateadd(day, -3, to_date(sysdate()))
{% else %}
where coalesce(cm.date, lm.date, bf.date, tsm.date) >= '2025-04-20'
{% endif %}
