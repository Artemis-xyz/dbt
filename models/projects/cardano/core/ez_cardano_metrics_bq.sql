{{
    config(
        materialized="table",
        database="cardano",
        schema="core",
        alias="ez_metrics_bq",
    )
}}

with
    blocks_data as (
        select
            date_trunc('day', block_time) as date,
            sum(tx_count) as txns,
            sum(sum_tx_fee) as fees_native,
            'cardano' as chain
        from {{ ref('fact_cardano_blocks') }}
        group by 1
    ),
    -- Get input addresses (from addresses)
    input_addresses as (
        select
            date_trunc('day', t.block_time) as date,
            json_value(tx_ins.outputs[0], '$.out_address') as address
        from {{ ref('fact_cardano_transactions') }} t
        inner join {{ ref('fact_cardano_transaction_inputs') }} tx_in
            on t.tx_hash = tx_in.tx_hash
        inner join {{ ref('fact_cardano_transaction_outputs') }} tx_ins
            on tx_in.input_tx_hash = tx_ins.tx_hash
            and tx_in.input_index = tx_ins.output_index
    ),
    -- Get output addresses (to addresses)
    output_addresses as (
        select
            date_trunc('day', block_time) as date,
            json_value(outputs[0], '$.out_address') as address
        from {{ ref('fact_cardano_transactions') }}
    ),
    -- Combine unique addresses for each day
    unique_daily_addresses as (
        select date, address
        from input_addresses
        union distinct
        select date, address
        from output_addresses
    ),
    -- Calculate daily active addresses (DAA)
    daily_active_addresses as (
        select
            date,
            count(distinct address) as daa
        from unique_daily_addresses
        group by 1
    ),
    -- Calculate rolling metrics (WAA/MAA) 
    rolling_address_metrics as (
        select 
            curr_day.date,
            -- Weekly Active Addresses (WAA)
            count(distinct case 
                when prev_day.date >= dateadd('day', -6, curr_day.date)  -- Last 7 days including current day
                then prev_day.address 
            end) as waa,
            -- Monthly Active Addresses (MAA)
            count(distinct case 
                when prev_day.date >= dateadd('day', -29, curr_day.date)  -- Last 30 days including current day
                then prev_day.address 
            end) as maa
        from (select distinct date from unique_daily_addresses) curr_day
        left join unique_daily_addresses prev_day 
            on prev_day.date <= curr_day.date 
            and prev_day.date >= dateadd('day', -29, curr_day.date)  -- Join on full 30-day window
        group by curr_day.date
    ),
    -- Get staking rewards data
    staking_rewards as (
        select
            date_trunc('day', block_time) as date,
            sum(reward_amount) as staking_rewards
        from {{ ref('fact_cardano_epoch_stake') }}
        group by 1
    ),
    -- Calculate circulating supply
    supply_data as (
        select
            date,
            -- Initial supply + cumulative staking rewards - cumulative fees
            31112484646 + 
            sum(coalesce(staking_rewards, 0)) over (order by date) - 
            sum(coalesce(fees_native, 0)) over (order by date) as circulating_supply
        from blocks_data b
        left join staking_rewards s on b.date = s.date
    ),
    price_data as ({{ get_coingecko_metrics("cardano") }}),
    github_data as ({{ get_github_metrics("cardano") }})
select
    b.date,
    b.chain,
    b.txns,
    coalesce(d.daa, 0) as daa,
    coalesce(r.waa, 0) as waa,
    coalesce(r.maa, 0) as maa,
    b.fees_native,
    b.fees_native * p.price as fees,
    b.fees_native / nullif(b.txns, 0) as avg_txn_fee,
    -- Standardized Metrics
    -- Market Data
    p.price,
    p.price * s.circulating_supply as market_cap,
    p.price * 45000000000 as fdmc,  -- FDMC uses total supply
    -- Cash Flow Metrics
    b.fees_native as chain_fees,
    b.fees_native * p.price as gross_protocol_revenue,
    b.fees_native as gross_protocol_revenue_native,
    -- github metrics
    github_data.weekly_commits_core_ecosystem,
    github_data.weekly_commits_sub_ecosystem,
    github_data.weekly_developers_core_ecosystem,
    github_data.weekly_developers_sub_ecosystem
from blocks_data b
left join daily_active_addresses d on b.date = d.date
left join rolling_address_metrics r on b.date = r.date
left join supply_data s on b.date = s.date
left join price_data p on b.date = p.date
left join github_data g on b.date = g.date
where b.date < current_date() 