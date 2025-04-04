{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_ethereum_staking_rewards",
    )
}}

with
    v2_rewards_raw as (
        select
            t.block_timestamp::date as date
            , sum(amount) as link
        from ethereum_flipside.core.ez_token_transfers t
        left join ethereum_flipside.core.ez_decoded_event_logs l on t.tx_hash = l.tx_hash
        where lower(t.from_address) = lower('0x996913c8c08472f584ab8834e925b06D0eb1D813') --rewards vault contract address
            and l.topics[0]::string = '0x106f923f993c2149d49b4255ff723acafa1f2d94393f561d3eda32ae348f7241'
            and lower(l.contract_address) = lower('0x996913c8c08472f584ab8834e925b06D0eb1D813')
        group by 1
        order by 1 desc
    )
    , v1_rewards_raw as (
        select
            block_timestamp
            , tx_hash
            , decoded_log:"staker"::string as staker
            , decoded_log:"principal"::number as principal
            , decoded_log:"baseReward"::number as base_reward
            , decoded_log:"delegationReward"::number as delegate_reward
        from ethereum_flipside.core.ez_decoded_event_logs 
        where topics[0]::string = '0x667838b33bdc898470de09e0e746990f2adc11b965b7fe6828e502ebc39e0434'
    )
    , all_rewards as (
        select
            block_timestamp::date as date
            , SUM(base_reward + delegate_reward)/1e18 as rewards
        from v1_rewards_raw
        group by 1
    
        union all
    
        select
            date
            , sum(link) as rewards
        from v2_rewards_raw
        group by 1
    )
    , prices as ({{get_coingecko_price_with_latest('chainlink')}})
    , datly_rewards as (
        select 
            all_rewards.date
            , sum(rewards) as staking_rewards_native
        from all_rewards 
        group by 1
    )
select 
    datly_rewards.date
    , 'ethereum' as chain
    , staking_rewards_native
    , staking_rewards_native * prices.price as staking_rewards
from datly_rewards 
left join prices using(date)