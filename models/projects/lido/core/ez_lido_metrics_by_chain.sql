{{
    config(
        materialized="table",
        snowflake_warehouse="LIDO",
        database="lido",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    fees_revenue_expenses as (
        SELECT
            date
            , block_rewards
            , mev_priority_fees
            , total_staking_yield as fees
            , operating_expenses
            , protocol_revenue
            , primary_supply_side_revenue
            , secondary_supply_side_revenue
            , total_supply_side_revenue
        FROM {{ ref('fact_lido_fees_revs_expenses') }}
    )
    , token_incentives_cte as (
        SELECT
            date
            , sum(amount_usd) as token_incentives
        FROM
            {{ ref('fact_lido_token_incentives') }}
        GROUP BY 1
    )
    , staked_eth_metrics as (
        select
            date
            , num_staked_eth
            , amount_staked_usd
            , num_staked_eth_net_change
            , amount_staked_usd_net_change
        from {{ ref('fact_lido_staked_eth_count_with_USD_and_change') }}
    )
    , treasury_cte as (
        SELECT
            date
            , sum(usd_balance) as treasury_value
        FROM
            {{ ref('fact_lido_dao_treasury') }}
        GROUP BY 1
    )
    , treasury_native_cte as (
        SELECT
            date
            , sum(native_balance) as treasury_native
        FROM
            {{ ref('fact_lido_dao_treasury') }}
        where token = 'LDO'
        GROUP BY 1
    )
    , net_treasury_cte as (
        SELECT
            date
            , sum(usd_balance) as net_treasury_value
        FROM {{ ref('fact_lido_dao_treasury') }}
        where token <> 'LDO'
        group by 1
    )
    , price_data as (
        {{ get_coingecko_metrics('lido-dao') }}
    )
    , tokenholder_cte as (
        SELECT
            date,
            token_holder_count
        FROM
            {{ ref('fact_ldo_tokenholder_count')}}
    )
select
    s.date
    , 'lido' as app
    , 'DeFi' as category
    , 'ethereum' as chain

    --Old metrics needed for compatibility
    , COALESCE(f.fees, 0) as fees
    , COALESCE(f.primary_supply_side_revenue, 0) as primary_supply_side_revenue
    , COALESCE(f.secondary_supply_side_revenue, 0) as secondary_supply_side_revenue
    , COALESCE(f.total_supply_side_revenue, 0) as total_supply_side_revenue
    , COALESCE(f.protocol_revenue, 0) as protocol_revenue
    , COALESCE(f.operating_expenses, 0) as operating_expenses
    , COALESCE(ti.token_incentives, 0) as token_incentives
    , token_incentives + operating_expenses as total_expenses
    , protocol_revenue - total_expenses as protocol_earnings
    , COALESCE(t.treasury_value, 0) as treasury_value
    , COALESCE(s.amount_staked_usd, 0) as net_deposits
    , COALESCE(s.num_staked_eth, 0) as outstanding_supply
    , COALESCE(s.amount_staked_usd, 0) as amount_staked_usd
    , COALESCE(s.num_staked_eth, 0) as num_staked_eth
    , COALESCE(s.amount_staked_usd_net_change, 0) as amount_staked_usd_net_change
    , COALESCE(s.num_staked_eth_net_change, 0) as num_staked_eth_net_change

    --Standardized Metrics

    --Usage Metrics
    , COALESCE(s.amount_staked_usd, 0) as tvl
    , COALESCE(s.num_staked_eth, 0) as tvl_native
    , COALESCE(s.amount_staked_usd_net_change, 0) as tvl_net_change
    , COALESCE(s.num_staked_eth_net_change, 0) as tvl_native_net_change

    --Cash Flow Metrics
    , COALESCE(f.mev_priority_fees, 0) as mev_priority_fees
    , COALESCE(f.block_rewards, 0) as block_rewards
    , COALESCE(f.fees, 0) as yield_generated
    , COALESCE(f.fees, 0) as ecosystem_revenue
    , COALESCE(f.fees, 0) * .90 as service_fee_allocation
    , COALESCE(f.fees, 0) * .05 as treasury_fee_allocation
    , COALESCE(f.fees, 0) * .05 as validator_fee_allocation

    --Treasury Metrics
    , COALESCE(t.treasury_value, 0) as treasury
    , COALESCE(tn.treasury_native, 0) as treasury_native
    , COALESCE(nt.net_treasury_value, 0) as net_treasury_value
    
from staked_eth_metrics s
left join fees_revenue_expenses f using(date)
left join treasury_cte t using(date)
left join treasury_native_cte tn using(date)
left join net_treasury_cte nt using(date)
left join token_incentives_cte ti using(date)
left join price_data p using(date)
left join tokenholder_cte th using(date)
where s.date < to_date(sysdate())