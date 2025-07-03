{{
    config(
        materialized="table",
        snowflake_warehouse="LIDO",
        database="lido",
        schema="core",
        alias="ez_metrics_by_token",
    )
}}

WITH
    fees AS (
        SELECT
            date
            , 'ETH' AS token
            , block_rewards_native
            , mev_priority_fees_native
            , fees_native
        FROM {{ ref('fact_lido_fees_revs_expenses') }}
    )
    , revenues_expenses AS (
        SELECT
            date
            , 'stETH' AS token
            , protocol_revenue_native
            , primary_supply_side_revenue_native
            , secondary_supply_side_revenue_native
            , total_supply_side_revenue_native
            , treasury_fee_allocation_native
            , validator_fee_allocation_native
        FROM {{ ref('fact_lido_fees_revs_expenses') }}
    )
    , token_incentives_cte AS (
        SELECT
            date
            , token
            , amount_native AS token_incentives_native
        FROM
            {{ ref('fact_lido_token_incentives') }}
    )
    , eth_deposited AS (
        SELECT
            date
            , 'ETH' AS token
            , num_staked_eth
        FROM {{ ref('fact_lido_staked_eth_count_with_USD_and_change') }}
    )
    , steth_outstanding AS (
        SELECT
            date
            , 'stETH' AS token
            , num_staked_eth AS outstanding_supply_native
        FROM {{ ref('fact_lido_staked_eth_count_with_USD_and_change') }}
    )
    , treasury_cte AS (
        SELECT
            date
            , token
            , SUM(native_balance) AS treasury_value_native
        FROM
            {{ ref('fact_lido_dao_treasury') }}
        GROUP BY 1, 2
    )
    , treasury_native_cte AS (
        SELECT
            date
            , token
            , SUM(native_balance) AS treasury_native
        FROM
            {{ ref('fact_lido_dao_treasury') }}
        WHERE token = 'LDO'
        GROUP BY 1, 2
    )
    , net_treasury_cte AS (
        SELECT
            date
            , token
            , SUM(native_balance) AS net_treasury_value_native
        FROM {{ ref('fact_lido_dao_treasury') }}
        WHERE token <> 'LDO'
        GROUP BY 1, 2
    )
SELECT
    date
    , token

    --Old metrics needed for compatibility
    , COALESCE(f.fees_native, 0) AS fees
    , COALESCE(primary_supply_side_revenue_native, 0) AS primary_supply_side_revenue
    , COALESCE(secondary_supply_side_revenue_native, 0) AS secondary_supply_side_revenue
    , COALESCE(total_supply_side_revenue_native, 0) AS total_supply_side_revenue
    , COALESCE(t.treasury_value_native, 0) AS treasury_value
    , COALESCE(s.num_staked_eth, 0) AS net_deposits
    , COALESCE(sto.outstanding_supply_native, 0) AS outstanding_supply

    --Standardized Metrics

    --Usage Metrics
    , COALESCE(s.num_staked_eth, 0) as lst_tvl_native
    , COALESCE(s.num_staked_eth, 0) as tvl_native

    --Cash Flow Metrics
    , COALESCE(f.mev_priority_fees_native, 0) as mev_priority_fees_native
    , COALESCE(f.block_rewards_native, 0) as block_rewards_native
    , COALESCE(f.fees_native, 0) as yield_generated_native
    , COALESCE(f.fees_native, 0) as fees_native
    , COALESCE(e.treasury_fee_allocation_native, 0) as treasury_fee_allocation_native
    , COALESCE(e.validator_fee_allocation_native, 0) as validator_fee_allocation_native

    -- Financial Statement Metrics
    , COALESCE(e.protocol_revenue_native, 0) as revenue_native
    , COALESCE(ti.token_incentives_native, 0) as token_incentives_native
    , COALESCE(ti.token_incentives_native, 0) as total_expenses_native
    , COALESCE(e.protocol_revenue_native, 0) - COALESCE(ti.token_incentives_native, 0) as earnings_native

    --Treasury Metrics
    , COALESCE(t.treasury_value_native, 0) as treasury_native
    , COALESCE(tn.treasury_native, 0) as own_token_treasury_native
    , COALESCE(nt.net_treasury_value_native, 0) as net_treasury_native
FROM fees f
FULL JOIN revenues_expenses e USING (date, token)
FULL JOIN treasury_cte t USING(date, token)
FULL JOIN treasury_native_cte tn USING(date, token)
FULL JOIN net_treasury_cte nt USING(date, token)
FULL JOIN token_incentives_cte ti USING(date, token)
LEFT JOIN steth_outstanding sto USING(date, token)
LEFT JOIN eth_deposited s USING(date, token)
WHERE date < to_date(sysdate())