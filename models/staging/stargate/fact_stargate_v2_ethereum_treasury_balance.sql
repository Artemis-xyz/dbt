{{
    config(
        materialized="table",
        unique_key="unique_id",
        snowflake_warehouse="MEDIUM",
    )
}}

with 
treasury_data as (
    {{ forward_filled_address_balances(
        artemis_application_id="stargate",
        type="treasury",
        chain="ethereum"
    )}}
)

, treasury_balances as (
    select
        date
        , case 
            when substr(t1.symbol, 0, 2) = 'S*' then 'stargate'
            when lower(treasury_data.contract_address) in (
                lower('0x72E95b8931767C79bA4EeE721354d6E99a61D004') -- variableDebtEthUSDC
                , lower('0x6df1C1E379bC5a00a7b4C6e67A203333772f45A8') --variableDebtEthUSDT
                , lower('0xeA51d7853EEFb32b6ee06b1C12E6dcCA88Be0fFE') --variableDebtEthWETH
                , lower('0x23878914EFE38d27C4D67Ab83ed1b93A74D4086a') --aEthUSDT
                , lower('0x98C23E9d8f34FEFb1B7BD6a91B7FF122F4e16F5c') --aEthUSDC   
                , lower('0xbcca60bb61934080951369a648fb03df4f96263c') --aUSDC
            ) then 'aave'
            else 'wallet'
        end as protocol        
        , treasury_data.contract_address
        , upper(replace(t1.symbol, 'S*', '')) as symbol
        , balance_native
        , balance
    from treasury_data
    inner join {{ ref("dim_coingecko_token_map") }} t1
        on lower(t1.contract_address) = lower(treasury_data.contract_address) and t1.chain = 'ethereum'
)

, aura_balance as (
    select 
        date
        , 'aura' as protocol
        , token_address as contract_address
        , token as symbol
        , token_balance as balance_native
        , tvl_token_adjusted as balance
    from {{ ref("fact_balancer_v2_ethereum_tvl_by_pool_and_token")}}
    where pool_address = lower('0x3ff3a210e57cFe679D9AD1e9bA6453A716C56a2e') 
)

, aura_rewards as (
     {{forward_filled_balance_for_address(
        chain="ethereum",
        address="0x8bd520Bf5d59F959b25EE7b78811142dDe543134"
    )}}
)

, aura_rewards_balance as (
    select 
        date
        , 'aura' as protocol
        , aura_rewards.contract_address
        , t1.symbol
        , balance_native
        , balance
    from aura_rewards
    inner join {{ ref("dim_coingecko_token_map") }} t1
        on lower(t1.contract_address) = lower(aura_rewards.contract_address) and t1.chain = 'ethereum'
    where aura_rewards.contract_address in (lower('0xba100000625a3754423978a60c9317c58a424e3D'))
)

, pancakeswap_pool as (
    {{forward_filled_balance_for_address(
        chain="ethereum",
        address="0x7524Fe020EDcD072EE98126b49Fa65Eb85F8C44C"
    )}}
)

, pancakeswap_balance as (
    select 
        date
        , 'pancakeswap' as protocol
        , pancakeswap_pool.contract_address
        , t1.symbol
        , balance_native
        , balance
    from pancakeswap_pool
    inner join {{ ref("dim_coingecko_token_map") }} t1
        on lower(t1.contract_address) = lower(pancakeswap_pool.contract_address) and t1.chain = 'ethereum'
    where pancakeswap_pool.contract_address in (lower('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'), lower('0xAf5191B0De278C7286d6C7CC6ab6BB8A73bA2Cd6'))
)

, curve_usdc_stg_pool_raw as (
    {{forward_filled_balance_for_address(
        "ethereum",
        "0x3211C6cBeF1429da3D0d58494938299C92Ad5860"
    )}}
)


, curve_usdc_stg_pool as (
    select
        date
        , curve_usdc_stg_pool_raw.contract_address
        , symbol
        , balance_native
        , balance
    from curve_usdc_stg_pool_raw
    inner join {{ ref("dim_coingecko_token_map") }} t1
        on lower(t1.contract_address) = lower(curve_usdc_stg_pool_raw.contract_address) and t1.chain = 'ethereum'
    where curve_usdc_stg_pool_raw.contract_address in (lower('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'), lower('0xAf5191B0De278C7286d6C7CC6ab6BB8A73bA2Cd6'))
)

, convex_ownership_in_curve_usdc_stg_gauge_raw as (
    {{forward_filled_token_balances(
        "ethereum",
        "0x95d16646311fDe101Eb9F897fE06AC881B7Db802"
    )}}
)

, convex_ownership_in_curve_usdc_stg_gauge as (
    select 
        date
        , sum(
            case
                when lower(address) = lower('0x989aeb4d175e16225e39e87d0d97a3360524ad80') then balance_raw
                else 0
            end
        ) / sum(balance_raw)
        as percent_ownership
    from convex_ownership_in_curve_usdc_stg_gauge_raw
    group by 1
)

, curve_usdc_stg_pool_token_balances as (
     {{forward_filled_token_balances(
        "ethereum",
        "0xdf55670e27bE5cDE7228dD0A6849181891c9ebA1"
    )}}
) 

, gauge_ownership_in_curve_usdc_stg_pool as (
    select 
        date
        , sum(
            case
                when lower(address) = lower('0x95d16646311fDe101Eb9F897fE06AC881B7Db802') then balance_raw
                else 0
            end
        ) / sum(balance_raw)
        as percent_ownership
    from curve_usdc_stg_pool_token_balances
    group by 1
)

, convex_3crv_stg_pool_raw as (
    {{forward_filled_balance_for_address(
        "ethereum",
        "0x867fe27FC2462cff8890B54DfD64E6d42a9D1aC8"
    )}}
)

, convex_3crv_stg_pool as (
    select
        date
        , convex_3crv_stg_pool_raw.contract_address
        , symbol
        , balance_native
        , balance
    from convex_3crv_stg_pool_raw
    inner join {{ ref("dim_coingecko_token_map") }} t1
        on lower(t1.contract_address) = lower(convex_3crv_stg_pool_raw.contract_address) and t1.chain = 'ethereum'
    where convex_3crv_stg_pool_raw.contract_address in (lower('0x3175Df0976dFA876431C2E9eE6Bc45b65d3473CC'), lower('0xAf5191B0De278C7286d6C7CC6ab6BB8A73bA2Cd6'))
)


, convex_balance_raw as (
    --convex_usdc_stg_balance
    select
        date
        , 'convex' as protocol
        , contract_address
        , symbol
        , balance_native * convex_ownership_in_curve_usdc_stg_gauge.percent_ownership * gauge_ownership_in_curve_usdc_stg_pool.percent_ownership as balance_native
        , balance * convex_ownership_in_curve_usdc_stg_gauge.percent_ownership * gauge_ownership_in_curve_usdc_stg_pool.percent_ownership as balance
    from curve_usdc_stg_pool
    inner join convex_ownership_in_curve_usdc_stg_gauge using (date)
    inner join gauge_ownership_in_curve_usdc_stg_pool using (date)

    union all

    --convex_3crv_stg_balance
    select
        date
        , 'convex' as protocol
        , contract_address
        , symbol
        , balance_native
        , balance
    from convex_3crv_stg_pool
)


, convex_balance as (
    select 
        date
        , protocol
        , contract_address
        , symbol
        , sum(balance_native) as balance_native
        , sum(balance) as balance
    from convex_balance_raw
    group by 1, 2, 3, 4
)

, stake_dao_ownership_in_curve_usdc_stg_gauge as (
    select 
        date
        , sum(
            case
                when lower(address) = lower('0x52f541764e6e90eebc5c21ff570de0e2d63766b6') then balance_raw
                else 0
            end
        ) / sum(balance_raw)
        as percent_ownership
    from convex_ownership_in_curve_usdc_stg_gauge_raw
    group by 1
)


, stake_dao_balance as (
    select
        date
        , 'stake_dao' as protocol
        , contract_address
        , symbol
        , balance_native * stake_dao_ownership_in_curve_usdc_stg_gauge.percent_ownership * gauge_ownership_in_curve_usdc_stg_pool.percent_ownership as balance_native
        , balance * stake_dao_ownership_in_curve_usdc_stg_gauge.percent_ownership * gauge_ownership_in_curve_usdc_stg_pool.percent_ownership as balance
    from curve_usdc_stg_pool
    inner join stake_dao_ownership_in_curve_usdc_stg_gauge using (date)
    inner join gauge_ownership_in_curve_usdc_stg_pool using (date)
)


, treasury_ownership_in_curve_usdc_stg_gauge as (
     select 
        date
        , sum(
            case
                when lower(address) = lower('0x65bb797c2b9830d891d87288f029ed8dacc19705') then balance_raw
                else 0
            end
        ) / sum(balance_raw)
        as percent_ownership
    from convex_ownership_in_curve_usdc_stg_gauge_raw
    group by 1
)

, curve_balance as (
    select
        date
        , 'curve' as protocol
        , contract_address
        , symbol
        , balance_native * treasury_ownership_in_curve_usdc_stg_gauge.percent_ownership * gauge_ownership_in_curve_usdc_stg_pool.percent_ownership as balance_native
        , balance * treasury_ownership_in_curve_usdc_stg_gauge.percent_ownership * gauge_ownership_in_curve_usdc_stg_pool.percent_ownership as balance
    from curve_usdc_stg_pool
    inner join treasury_ownership_in_curve_usdc_stg_gauge using (date)
    inner join gauge_ownership_in_curve_usdc_stg_pool using (date)
)


, yearn_fi_ownership_in_curve_usdc_stg_pool as (
    select 
        date
        , sum(
            case
                when lower(address) = lower('0x341bb10d8f5947f3066502dc8125d9b8949fd3d6') then balance_raw
                else 0
            end
        ) / sum(balance_raw)
        as percent_ownership
    from curve_usdc_stg_pool_token_balances
    group by 1
)

, yearn_fi_balance as (
    select
        date
        , 'yearn_fi' as protocol
        , contract_address
        , symbol
        , balance_native * yearn_fi_ownership_in_curve_usdc_stg_pool.percent_ownership as balance_native
        , balance * yearn_fi_ownership_in_curve_usdc_stg_pool.percent_ownership as balance
    from curve_usdc_stg_pool
    inner join yearn_fi_ownership_in_curve_usdc_stg_pool using (date)
)

, balances as (
    select * from treasury_balances
    union all 
    select * from aura_balance
    union all
    select * from aura_rewards_balance
    union all
    select * from pancakeswap_balance
    union all
    select * from convex_balance
    union all
    select * from stake_dao_balance
    union all
    select * from curve_balance
    union all
    select * from yearn_fi_balance
)

select 
    date
    , protocol
    , 'ethereum' as chain
    , contract_address
    , symbol
    , balance_native
    , balance
from balances