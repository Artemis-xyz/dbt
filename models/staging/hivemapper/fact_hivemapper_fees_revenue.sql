{{
    config(
        materialized="table",
        snowflake_warehouse="HIVEMAPPER"
    )
}}


with
    mapping_rewards as (
        select
            block_timestamp::date as date
            , 'mapping_rewards' as type
            , sum(coalesce(post_token_balances[0]:"uiTokenAmount":"uiAmount"::number, 0) - coalesce(pre_token_balances[0]:"uiTokenAmount":"uiAmount"::number, 0)) as mapping_rewards
        from {{ ref('fact_hivemapper_mapping_reward_transactions') }}
        group by 1
    )
    , burns as (
        select 
            block_timestamp::date as date
            , 'burn' as type
            , sum(coalesce(burn_amount, 0) / pow(10, decimal)) as burn_amount
        from solana_flipside.defi.fact_token_burn_actions 
        where mint = '4vMsoUT2BWatFweudnQM1xedRLfJgJ7hswhcpz4xgBTy'
            and block_timestamp > '2022-10-31'
        group by 1
    )
    , mints as (
        select
            block_timestamp::date as date
            , 'mint' as type
            , sum(coalesce(mint_amount, 0) / pow(10, decimal)) as mint_amount
        from solana_flipside.defi.fact_token_mint_actions   
        where mint = '4vMsoUT2BWatFweudnQM1xedRLfJgJ7hswhcpz4xgBTy'
            and block_timestamp > '2022-10-31'  
        group by 1  
    )
    , prices as ({{ get_coingecko_price_with_latest("hivemapper") }})

select
    mints.date
    , mints.mint_amount as rewards_native
    , mints.mint_amount * prices.price as rewards
    , mapping_rewards.mapping_rewards as mapping_rewards_native
    , mapping_rewards.mapping_rewards * prices.price as mapping_rewards
    , mints.mint_amount - coalesce(mapping_rewards.mapping_rewards, 0) as qa_rewards_native
    , (mints.mint_amount - coalesce(mapping_rewards.mapping_rewards, 0)) * prices.price as qa_rewards
    , burns.burn_amount as fees_native
    , burns.burn_amount * prices.price as fees
    , fees_native as revenue_native
    , fees as revenue
from mints
left join mapping_rewards using(date)
left join burns using(date)
left join prices on mints.date = prices.date