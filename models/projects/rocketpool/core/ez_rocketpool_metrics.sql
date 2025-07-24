{{
    config(
        materialized="incremental",
        snowflake_warehouse="ROCKETPOOL",
        database="rocketpool",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    staked_eth_metrics as (
        select
            date
            , num_staked_eth
            , amount_staked_usd
            , num_staked_eth_net_change
            , amount_staked_usd_net_change
        from {{ ref('fact_rocketpool_staked_eth_count_with_USD_and_change') }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    )
    , fees_revs_cte as (
        select
            date
            , cl_rewards_usd
            , el_rewards_usd
            , deposit_fee_usd as deposit_fees
            , total_node_rewards_usd + deposit_fees as fees
            , total_node_rewards_usd as primary_supply_side_revenue
            , deposit_fees as secondary_supply_side_revenue
            , total_node_rewards_usd + deposit_fees as total_supply_side_revenue
        from {{ ref('fact_rocketpool_fees_revs') }}
        left join {{ ref('fact_rocketpool_deposit_fees') }} d using(date)
        {{ ez_metrics_incremental('date', backfill_date) }}
    )
    , token_incentives_cte as (
        SELECT
            date
            , token_incentives_usd
        FROM
            {{ ref('fact_rocketpool_expenses') }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    )
    , outstanding_supply_cte as (
        SELECT
            date
            , reth_supply
        FROM
            {{ ref('fact_reth_outstanding') }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    )
    , treasury_cte as (
        SELECT
            date
            , sum(usd_balance) as treasury_value
        FROM
            {{ ref('fact_rocketpool_treasury') }}
        {{ ez_metrics_incremental('date', backfill_date) }}
        GROUP BY 1
    )
    , treasury_native_cte as (
        SELECT
            date
            , native_balance as treasury_native
        FROM
            {{ ref('fact_rocketpool_treasury') }}
        {{ ez_metrics_incremental('date', backfill_date) }}
        AND token = 'RPL'
    )
    , net_treasury_cte as (
        SELECT
            date
            , sum(usd_balance) as net_treasury_value
        FROM
            {{ ref('fact_rocketpool_treasury') }}
        {{ ez_metrics_incremental('date', backfill_date) }}
        AND token <> 'RPL'
        GROUP BY 1
    )
    , prices_cte as (
        {{ get_coingecko_metrics('rocket-pool')}}
    )
    , token_holders_cte as (
        SELECT
            date
            , token_holder_count
        FROM
            {{ ref('fact_rocketpool_token_holders') }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    )
select
    p.date

    --Old metrics needed for compatibility
    , COALESCE(f.cl_rewards_usd, 0) as cl_rewards_usd
    , COALESCE(f.el_rewards_usd, 0) as el_rewards_usd
    , COALESCE(f.primary_supply_side_revenue, 0) as primary_supply_side_revenue
    , COALESCE(f.secondary_supply_side_revenue, 0) as secondary_supply_side_revenue
    , COALESCE(f.total_supply_side_revenue, 0) as total_supply_side_revenue
    , staked_eth_metrics.num_staked_eth as net_deposits
    , os.reth_supply as outstanding_supply
    , COALESCE(t.treasury_value, 0) as treasury_value

    --Standardized Metrics

    --Market Metrics
    , COALESCE(p.fdmc, 0) as fdmc
    , COALESCE(p.market_cap, 0) as market_cap
    , COALESCE(p.token_volume, 0) as token_volume

    --Usage Metrics
    , staked_eth_metrics.num_staked_eth as tvl_native
    , staked_eth_metrics.num_staked_eth as lst_tvl_native
    , staked_eth_metrics.amount_staked_usd as tvl
    , staked_eth_metrics.amount_staked_usd as lst_tvl
    , staked_eth_metrics.num_staked_eth_net_change as lst_tvl_native_net_change
    , staked_eth_metrics.amount_staked_usd_net_change as lst_tvl_net_change

    --Cash Flow Metrics
    , COALESCE(f.cl_rewards_usd, 0) as block_rewards
    , COALESCE(f.el_rewards_usd, 0) as mev_priority_fees
    , COALESCE(f.deposit_fees, 0) as lst_deposit_fees
    , COALESCE(f.cl_rewards_usd, 0) + COALESCE(f.el_rewards_usd, 0) as yield_generated
    , COALESCE(f.fees, 0) as fees
    , yield_generated * 0.14 as validator_fee_allocation
    , yield_generated * 0.86 as service_fee_allocation

    --Financial Statement Metrics
    , 0 as revenue
    , COALESCE(ti.token_incentives_usd, 0) as token_incentives
    , 0 as operating_expenses
    , COALESCE(token_incentives_usd, 0) as total_expenses
    , revenue - token_incentives as earnings

    --Treasury Metrics
    , COALESCE(t.treasury_value, 0) as treasury
    , COALESCE(tn.treasury_native, 0) as treasury_value_native
    , COALESCE(nt.net_treasury_value, 0) as net_treasury_value
    
    --Other Metrics
    , COALESCE(p.token_turnover_circulating, 0) as token_turnover_circulating
    , COALESCE(p.token_turnover_fdv, 0) as token_turnover_fdv
    , COALESCE(th.token_holder_count, 0) as token_holder_count

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from prices_cte p
left join fees_revs_cte f using(date)
left join staked_eth_metrics using(date)
left join token_incentives_cte ti using(date)
left join treasury_cte t using(date)
left join treasury_native_cte tn using(date)
left join net_treasury_cte nt using(date)
left join outstanding_supply_cte os using(date)
left join token_holders_cte th using(date)
{{ ez_metrics_incremental('date', backfill_date) }}
and p.date < to_date(sysdate())
