{{ config(materialized="incremental", unique_key="date") }}

with coin_creations as (
    select
        block_timestamp,
        caller_address,
        payout_recipient_address,
        platform_referrer_address,
        coin_address
    from {{ ref('fact_zora_base_launchpad_events') }}
    {% if is_incremental() %}
        where block_timestamp >= (select dateadd(day, -30, max(date)) from {{ this }})
    {% endif %}
),

daily_aggregates as (
    select
        date_trunc('day', block_timestamp) as date,
        
        -- Core Fundamental Metrics
        count(distinct coin_address) as tokens_created,
        count(distinct caller_address) as unique_creators,
        count(distinct payout_recipient_address) as unique_payout_recipients,
        count(distinct platform_referrer_address) as unique_platform_referrers,
        
        -- Daily Active Addresses (Token Creators)
        count(distinct caller_address) as daily_active_addresses
        
    from coin_creations
    group by 1
),

cumulative_metrics as (
    select
        date,
        tokens_created,
        unique_creators,
        unique_payout_recipients,
        unique_platform_referrers,
        daily_active_addresses,
        
        -- Cumulative Totals
        sum(tokens_created) over (order by date) as cumulative_tokens_created,
        sum(unique_creators) over (order by date) as cumulative_unique_creators
        
    from daily_aggregates
)

select * from cumulative_metrics
order by date desc