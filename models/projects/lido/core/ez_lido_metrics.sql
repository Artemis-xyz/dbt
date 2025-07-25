{{
    config(
        materialized="incremental",
        snowflake_warehouse="LIDO",
        database="lido",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    fees_revenue_expenses as (
        select
            date
            , coalesce(block_rewards, 0) as block_rewards
            , coalesce(mev_priority_fees, 0) as mev_priority_fees
            , coalesce(total_staking_yield, 0) as yield_generated
            , coalesce(fees, 0) as fees
            , coalesce(validator_fee_allocation, 0) as validator_fee_allocation
            , coalesce(treasury_fee_allocation, 0) as treasury_fee_allocation
            , coalesce(protocol_revenue, 0) as protocol_revenue
            , coalesce(primary_supply_side_revenue, 0) as primary_supply_side_revenue
            , coalesce(secondary_supply_side_revenue, 0) as secondary_supply_side_revenue
            , coalesce(total_supply_side_revenue, 0) as total_supply_side_revenue
        from {{ ref('fact_lido_fees_revs_expenses') }}
    )
    , staked_eth_metrics as (
        select
            date
            , coalesce(num_staked_eth, 0) as num_staked_eth
            , coalesce(amount_staked_usd, 0) as amount_staked_usd
            , coalesce(num_staked_eth_net_change, 0) as num_staked_eth_net_change
            , coalesce(amount_staked_usd_net_change, 0) as amount_staked_usd_net_change
        from {{ ref('fact_lido_staked_eth_count_with_USD_and_change') }}
    )
    , treasury_cte as (
        select
            date
            , coalesce(sum(usd_balance), 0) as treasury_value
        from {{ ref('fact_lido_dao_treasury') }}
        group by 1
    )
    , treasury_native_cte as (
        select
            date
            , coalesce(sum(native_balance), 0) as treasury_native
        from {{ ref('fact_lido_dao_treasury') }}
        where token = 'LDO'
        group by 1
    )
    , net_treasury_cte as (
        select
            date
            , coalesce(sum(usd_balance), 0) as net_treasury_value
        FROM {{ ref('fact_lido_dao_treasury') }}
        WHERE token <> 'LDO'
        group by 1
    )
    , token_incentives_cte as (
        select
            date
            , coalesce(sum(amount_usd), 0) as token_incentives
        from
            {{ ref('fact_lido_token_incentives') }}
        group by 1
    )
    , market_metrics as (
        {{ get_coingecko_metrics('lido-dao') }}
    )
    , tokenholder_cte as (
        select
            date,
            coalesce(token_holder_count, 0) as token_holder_count
        from
            {{ ref('fact_ldo_tokenholder_count')}}
    )
select
    staked_eth_metrics.date
    , 'lido' as artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_metrics.price
    , market_metrics.fdmc
    , market_metrics.market_cap
    , market_metrics.token_volume
    
    -- Usage Data
    , staked_eth_metrics.amount_staked_usd as lst_tvl
    , staked_eth_metrics.amount_staked_usd as tvl
    , staked_eth_metrics.num_staked_eth as lst_tvl_native
    , staked_eth_metrics.num_staked_eth as tvl_native
    , staked_eth_metrics.amount_staked_usd_net_change as lst_tvl_net_change
    , staked_eth_metrics.num_staked_eth_net_change as lst_tvl_native_net_change
    , fees_revenue_expenses.yield_generated

    -- Fees Data
    , fees_revenue_expenses.mev_priority_fees
    , fees_revenue_expenses.block_rewards
    , fees_revenue_expenses.fees
    , fees_revenue_expenses.treasury_fee_allocation
    , fees_revenue_expenses.validator_fee_allocation

    -- Financial Statement
    , fees_revenue_expenses.protocol_revenue as revenue
    , token_incentives_cte.token_incentives
    , token_incentives_cte.token_incentives as total_expenses
    , fees_revenue_expenses.protocol_revenue - token_incentives_cte.token_incentives as earnings

    --Treasury Data
    , treasury_cte.treasury_value
    , treasury_native_cte.treasury_native
    , net_treasury_cte.net_treasury_value

    -- Token Turnover/Other Data
    , market_metrics.token_turnover_fdv
    , market_metrics.token_turnover_circulating
    , tokenholder_cte.token_holder_count

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from staked_eth_metrics
left join fees_revenue_expenses using(date)
left join treasury_cte using(date)
left join treasury_native_cte using(date)
left join net_treasury_cte using(date)
left join token_incentives_cte using(date)
left join market_metrics using(date)
left join tokenholder_cte using(date)
where true
{{ ez_metrics_incremental('staked_eth_metrics.date', backfill_date) }}
and staked_eth_metrics.date < to_date(sysdate())