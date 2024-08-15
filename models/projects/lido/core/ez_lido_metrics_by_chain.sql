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
            , amount_usd as token_incentives
        FROM
            {{ ref('fact_lido_token_incentives') }}
    )
    , staked_eth_metrics as (
        select
            date
            , num_staked_eth
            , amount_staked_usd
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
    , fdv_turnover_cte as (
        {{ get_fdv_and_token_turnover('lido-dao') }}
    )
    , tokenholder_cte as (
        {{ token_holders('ethereum','0x5a98fcbea516cf06857215779fd812ca3bef1b32',  '2021-01-04')}}
    )
select
    s.date
    , 'ethereum' as chain
    , f.mev_priority_fees
    , f.block_rewards
    , f.fees
    , f.primary_supply_side_revenue
    , f.secondary_supply_side_revenue
    , f.total_supply_side_revenue
    , f.protocol_revenue
    , f.operating_expenses
    , ti.token_incentives
    , f.protocol_revenue - f.operating_expenses - COALESCE(ti.token_incentives, 0) as protocol_earnings
    , t.treasury_value
    , tn.treasury_native
    , nt.net_treasury_value
    , s.amount_staked_usd as net_deposits
    , s.num_staked_eth as outstanding_supply
    , s.amount_staked_usd as tvl
    , ft.fdmc
    , ft.market_cap
    , ft.token_volume
    , ft.token_turnover_fdv
    , ft.token_turnover_circulating
    , th.token_holder_count
from fees_revenue_expenses f
left join staked_eth_metrics s using(date)
left join treasury_cte t using(date)
left join treasury_native_cte tn using(date)
left join net_treasury_cte nt using(date)
left join token_incentives_cte ti using(date)
left join fdv_turnover_cte ft using(date)
left join tokenholder_cte th using(date)
where s.date < current_date()