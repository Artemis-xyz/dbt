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
        count(distinct caller_address) as daily_active_creators
        
    from coin_creations
    group by 1
),

daily_creators as (
    select
        date_trunc('day', block_timestamp) as date,
        caller_address
    from coin_creations
    group by 1, 2
),

new_creators_per_day as (
    select
        date,
        count(distinct caller_address) as new_creators
    from daily_creators dc1
    where not exists (
        select 1 
        from daily_creators dc2 
        where dc2.caller_address = dc1.caller_address 
        and dc2.date < dc1.date
    )
    group by 1
),

cumulative_metrics as (
    select
        da.date,
        da.daily_active_creators,
        da.tokens_created,
        da.unique_creators,
        da.unique_payout_recipients,
        da.unique_platform_referrers,
        coalesce(nc.new_creators, 0) as new_creators_per_day,
        
        -- Cumulative Totals
        sum(da.tokens_created) over (order by da.date) as cumulative_tokens_created,
        sum(da.unique_creators) over (order by da.date) as cumulative_unique_creators
        
    from daily_aggregates da
    left join new_creators_per_day nc on da.date = nc.date
)

select * from cumulative_metrics
order by date desc