{{ config(
    materialized="table",
    snowflake_warehouse="MARINADE"
) }}

with v1_commissions as (
    select 
        date(block_timestamp) as date
        , sum(deposit_amount) * 0.0042 as fees_native
    from solana_flipside.marinade.ez_liquid_staking_actions
    where 
        date(block_timestamp) < '2024-08-18'
    group by date(block_timestamp)
    order by date(block_timestamp) desc
),
v1_unstake_fees as (
    select
        date(block_timestamp) as date
        , sum(decoded_args:msolAmount::numeric / 1e9) as msol_native
    from solana_flipside.core.ez_events_decoded
    where 
        program_id = 'MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD' 
        and event_type = 'liquidUnstake' 
        and date(block_timestamp) < '2024-08-18'
    group by date(block_timestamp)
    order by date(block_timestamp) desc
),
v1_liquidity_pool as (
    select 
        date(block_timestamp) as date
        , 'SOL' as token
        , sum(case when action_type = 'deposit' then token_b_amount else 0 end) - 
        sum(case when action_type = 'withdraw' then token_b_amount else 0 end) as net_liquidity
    from solana_flipside.marinade.ez_liquidity_pool_actions
    where pool_address = 'EGyhb2uLAsRUbRx9dNFBjMVYnFaASWMvD6RE1aEf2LxL' 
        and date(block_timestamp) < '2024-08-18'
    group by date(block_timestamp)
),
v1_liquidity_pool_cumulative as (
    select 
        date
        , sum(net_liquidity) over (order by date) as total_liquidity
    from v1_liquidity_pool
), date_spine as (
    select
        date
    from
        pc_dbt_db.prod.dim_date_spine
    where date between (select min(date) from v1_liquidity_pool_cumulative) and to_date(sysdate())
),
all_dates as (
    select
        ds.date,
        coalesce(last_value(tb.total_liquidity IGNORE NULLS) over (
            order by ds.date rows between unbounded preceding and current row
        ), 0) AS total_liquidity
    from
        date_spine ds
    left join
        v1_liquidity_pool_cumulative tb on date(ds.date) = date(tb.date)
    where ds.date < '2024-08-18'
    order by
        ds.date
),
unstaking_fees_calculation as (
    select
        c.date
        , c.total_liquidity as amount_after
        , 9 - (9 - 0.1) * (c.total_liquidity / 106000) AS unstake_fee
        , uf.msol_native * (9 - (9 - 0.1) * (c.total_liquidity / 106000)) AS unstaking_fees
    from all_dates c
    left join v1_unstake_fees uf using (date)
    order by c.date desc
),
v1_fees as (    
    select 
        date
        , coalesce(unstaking_fees, 0) as unstaking_fees
        , coalesce(fees_native, 0) as fees_native
    from unstaking_fees_calculation
    left join v1_commissions using (date)
)
select 
    date
    , unstaking_fees
    , fees_native
from v1_fees
order by date desc