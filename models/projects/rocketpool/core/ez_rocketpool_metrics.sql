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
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
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
    )
    , token_incentives_cte as (
        SELECT
            date
            , token_incentives_usd
        FROM
            {{ ref('fact_rocketpool_expenses') }}
    )
    , outstanding_supply_cte as (
        SELECT
            date
            , reth_supply
        FROM
            {{ ref('fact_reth_outstanding') }}
    )
    , treasury_cte as (
        SELECT
            date
            , sum(usd_balance) as treasury_value
        FROM
            {{ ref('fact_rocketpool_treasury') }}
        GROUP BY 1
    )
    , treasury_native_cte as (
        SELECT
            date
            , native_balance as treasury_native
        FROM
            {{ ref('fact_rocketpool_treasury') }}
        WHERE token = 'RPL'
    )
    , net_treasury_cte as (
        SELECT
            date
            , sum(usd_balance) as net_treasury_value
        FROM
            {{ ref('fact_rocketpool_treasury') }}
        WHERE token <> 'RPL'
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
    )
select
    p.date
    , 'rocketpool' as artemis_id

    -- Standardized Metrics
    -- Market Metrics
    , p.price
    , p.market_cap
    , p.fdmc
    , p.token_volume

    --Usage Metrics
    , staked_eth_metrics.num_staked_eth as tvl_native
    , staked_eth_metrics.num_staked_eth as lst_tvl_native
    , staked_eth_metrics.amount_staked_usd as tvl
    , staked_eth_metrics.amount_staked_usd as lst_tvl
    , staked_eth_metrics.num_staked_eth_net_change as lst_tvl_native_net_change
    , staked_eth_metrics.amount_staked_usd_net_change as lst_tvl_net_change

    --Fee Metrics
    , f.cl_rewards_usd as block_rewards
    , f.el_rewards_usd as mev_priority_fees
    , f.deposit_fees as lst_deposit_fees
    , coalesce(f.cl_rewards_usd, 0) + coalesce(f.el_rewards_usd, 0) as yield_generated
    , f.fees as fees
    , coalesce(yield_generated, 0) * 0.14 as validator_fee_allocation
    , coalesce(yield_generated, 0) * 0.86 as service_fee_allocation

    --Financial Metrics
    , 0 as revenue
    , ti.token_incentives_usd as token_incentives
    , 0 as operating_expenses
    , ti.token_incentives_usd as total_expenses
    , 0 - coalesce(ti.token_incentives_usd, 0) as earnings

    --Treasury Metrics
    , t.treasury_value as treasury
    , tn.treasury_native as treasury_value_native
    , nt.net_treasury_value as net_treasury_value
    
    --Other Metrics
    , p.token_turnover_circulating
    , p.token_turnover_fdv
    , th.token_holder_count

    -- Timestamp Columns
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
where true
{{ ez_metrics_incremental('date', backfill_date) }}
and p.date < to_date(sysdate())
