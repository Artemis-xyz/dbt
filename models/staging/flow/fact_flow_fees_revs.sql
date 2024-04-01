{{ config(materialized="table") }}
with
    flow_fees as (
        select
            date_trunc('day', block_timestamp) as block_date,
            event_type,
            sum(event_data:amount::float) as total_fees
        from flow_flipside.core.fact_events
        where event_type = 'FeesDeducted'
        group by block_date, event_type
    ),
    flow_revs as (
        select
            date_trunc('day', block_timestamp) as block_date,
            event_type,
            sum(event_data:feesburned::float) as fees_burned
        from flow_flipside.core.fact_events
        where event_type = 'EpochTotalRewardsPaid'
        group by block_date, event_type
    ),
    flow_daily_prices as (
        select date, shifted_token_price_usd as usd_price
        from pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold
        where coingecko_id = 'flow'
    ),
    combined_data as (
        select
            p.date,
            coalesce(f.total_fees, 0) as total_fees_flow,
            coalesce(f.total_fees, 0) * p.usd_price as total_fees_usd,
            coalesce(r.fees_burned, 0) as fees_burned,
            coalesce(r.fees_burned, 0) * p.usd_price as fees_burned_usd
        from flow_daily_prices p
        left join flow_fees f on f.block_date = p.date
        left join flow_revs r on r.block_date = p.date
    )
select *
from combined_data
order by date desc
