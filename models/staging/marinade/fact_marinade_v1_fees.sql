{{ config(
    materialized="table",
    snowflake_warehouse="MARINADE"
) }}

with marinade_unstake_txns as (
    select 
        block_timestamp
        , tx_id
        , signers[0]::string as burn_sol_authority
        , signers[1]::string as ticket_account
        , decoded_args:msolAmount / 1e9 as msol_burn_native
    from solana_flipside.core.ez_events_decoded
    where 1=1
        and event_type = 'orderUnstake'
        and program_id = 'MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD'
        and succeeded = true
)

, marinade_claims_txns as (
    select
        decoded_events.block_timestamp
        , decoded_events.tx_id
        , decoded_events.signers[0]::string as to_address
        , instruction:parsed:info:lamports::float / 1e9 as claim_sol_native
        , decoded_accounts[2]:pubkey::string as ticket_account
    from solana_flipside.core.ez_events_decoded decoded_events
    left join solana_flipside.core.fact_events_inner inner_events on decoded_events.tx_id = inner_events.tx_id
    where 1=1
        and decoded_events.succeeded = true
        and decoded_events.event_type = 'claim'
        and decoded_events.program_id = 'MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD'
)

-- calculate commissions fees from rewards (claim_sol_native - msol_burn_native = rewards)
, combined_txns as (
    select
        u.tx_id
        , u.block_timestamp
        , u.msol_burn_native
        , c.claim_sol_native
        , u.burn_sol_authority
        , (c.claim_sol_native - u.msol_burn_native) as net_claim_native
        , (c.claim_sol_native - u.msol_burn_native) * 0.0042 as fees_native
    from marinade_unstake_txns u
    join marinade_claims_txns c
        on u.ticket_account = c.ticket_account
    where (c.claim_sol_native - u.msol_burn_native) > 0
)

, v1_commissions as (
    select
        date(block_timestamp) as date
        , sum(fees_native) as fees_native
    from combined_txns
    group by date(block_timestamp)
)

, v1_unstake_fees as (
    select
        date(block_timestamp) as date
        , sum(decoded_args:msolAmount::numeric / 1e9) as msol_native
    from solana_flipside.core.ez_events_decoded
    where 
        program_id = 'MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD' 
        and event_type = 'liquidUnstake' and succeeded = true
        and date(block_timestamp) < '2025-03-07'
    group by date(block_timestamp)
    order by date(block_timestamp) desc
)

, v1_liquidity_pool as (
    select 
        date(block_timestamp) as date,
        sum(case when action_type = 'deposit' then token_b_amount
                else 0 
            end
        ) - 
        sum(case  when action_type = 'withdraw' then token_b_amount
                else 0 
            end
        ) as net_liquidity
    from solana_flipside.marinade.ez_liquidity_pool_actions
    where pool_address in ('EGyhb2uLAsRUbRx9dNFBjMVYnFaASWMvD6RE1aEf2LxL' -- Raydium (mSOL-WSOL) Market
                            , 'B89LeajR1xkMd2JmrESB5c6VRwLpm24miPE7JTvFkTqa' -- Raydium (MNDE-WSOL) Market
                            , 'B1BbVGNjkt98FQZBhe26auV8fGMqwt6s4VRm6iwNJXvD' -- Meteora (mSOL-WSOL) Market
                            , 'AgJLUgJRMvWqGbDXsMxbCb6m1gYrqt2PAfQrLAi3H6XT' -- Meteora (MNDE-WSOL) Market
                            , '4kpvTZLvNvUknsovSTrjUacwLUrwYbvZVaNUeDFg9GBK' -- Meteora (mSOL-WSOL) Market
                            , '7VbZgGnf3xYnQ6Vejh5to4fm83vrbt1Sy9qVPbv9V6qu') -- Meteora (mSOL-SOL) Market
    and date(block_timestamp)  < '2025-03-07'
    group by date
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
    where ds.date < '2025-03-07'
    order by
        ds.date
),
unstaking_fees_calculation as (
    select
        c.date
        , c.total_liquidity as amount_after
            , case 
                when c.total_liquidity > 106000 then 0.001
                when c.date < '2022-12-10' then 0.03 - (0.03 - 0.001) * (c.total_liquidity / 106000)
                else 0.09 - (0.09 - 0.001) * (c.total_liquidity / 106000)
            end as 
            unstake_fee_percentage
        , uf.msol_native * (
            case 
                -- Cap fee at 0.3% or 0.1% if total_liquidity > 106,000
                when c.total_liquidity > 100000 and c.date < '2023-01-27' then 0.003
                when c.total_liquidity > 150000 and c.date >= '2023-01-27' and c.date < '2023-07-27' then 0.003
                when c.total_liquidity > 106000 and c.date >= '2023-07-27' and c.date <= '2025-03-07' then 0.001

                -- Before Jan 27, 2023 → 3% → 0.3%
                when c.date < '2023-01-27' then 
                    greatest(0.003, 0.03 - (0.03 - 0.003) * (c.total_liquidity / 100000))

                -- Jan 28 to July 26, 2023 → 9% → 0.3%
                when c.date >= '2023-01-27' and c.date < '2023-07-27' then 
                    greatest(0.003, 0.09 - (0.09 - 0.003) * (c.total_liquidity / 150000))

                -- July 27, 2023 to Mar 7, 2025 → 9% → 0.1%
                when c.date >= '2023-07-27' and c.date <= '2025-03-07' then 
                    greatest(0.001, 0.09 - (0.09 - 0.001) * (c.total_liquidity / 106000))

                -- Default fallback (can return 0 or NULL based on your preference)
                else 0
            end
        ) AS unstaking_fees
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