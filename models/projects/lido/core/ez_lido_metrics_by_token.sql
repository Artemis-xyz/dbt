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
            , coalesce(block_rewards_native, 0) as block_rewards_native
            , coalesce(mev_priority_fees_native, 0) as mev_priority_fees_native
            , coalesce(fees_native, 0) as fees_native
        FROM {{ ref('fact_lido_fees_revs_expenses') }}
    )
    , revenues_expenses AS (
        SELECT
            date
            , 'stETH' AS token
            , coalesce(protocol_revenue_native, 0) as protocol_revenue_native
            , coalesce(primary_supply_side_revenue_native, 0) as primary_supply_side_revenue_native
            , coalesce(secondary_supply_side_revenue_native, 0) as secondary_supply_side_revenue_native
            , coalesce(total_supply_side_revenue_native, 0) as total_supply_side_revenue_native
            , coalesce(treasury_fee_allocation_native, 0) as treasury_fee_allocation_native
            , coalesce(validator_fee_allocation_native, 0) as validator_fee_allocation_native
        FROM {{ ref('fact_lido_fees_revs_expenses') }}
    )
    , token_incentives_cte AS (
        SELECT
            date
            , token
            , coalesce(amount_native, 0) AS token_incentives_native
        FROM
            {{ ref('fact_lido_token_incentives') }}
    )
    , eth_deposited AS (
        SELECT
            date
            , 'ETH' AS token
            , coalesce(num_staked_eth, 0) as num_staked_eth
        FROM {{ ref('fact_lido_staked_eth_count_with_USD_and_change') }}
    )
    , steth_outstanding AS (
        SELECT
            date
            , 'stETH' AS token
            , coalesce(num_staked_eth, 0) AS outstanding_supply_native
        FROM {{ ref('fact_lido_staked_eth_count_with_USD_and_change') }}
    )
    , treasury_cte AS (
        SELECT
            date
            , token
            , coalesce(SUM(native_balance), 0) AS treasury_value_native
        FROM
            {{ ref('fact_lido_dao_treasury') }}
        GROUP BY 1, 2
    )
    , treasury_native_cte AS (
        SELECT
            date
            , token
            , coalesce(SUM(native_balance), 0) AS treasury_native
        FROM
            {{ ref('fact_lido_dao_treasury') }}
        WHERE token = 'LDO'
        GROUP BY 1, 2
    )
    , net_treasury_cte AS (
        SELECT
            date
            , token
            , coalesce(SUM(native_balance), 0) AS net_treasury_value_native
        FROM {{ ref('fact_lido_dao_treasury') }}
        WHERE token <> 'LDO'
        GROUP BY 1, 2
    )
SELECT
    date
    , 'lido' as artemis_id
    , token

    -- Standardized Metrics

    -- Usage Data
    , eth_deposited.num_staked_eth as lst_tvl_native
    , eth_deposited.num_staked_eth as tvl_native

    -- Fee Data
    , fees.mev_priority_fees_native as mev_priority_fees_native
    , fees.block_rewards_native as block_rewards_native
    , fees.fees_native as yield_generated_native
    , fees.fees_native as fees_native
    , revenues_expenses.treasury_fee_allocation_native as treasury_fee_allocation_native
    , revenues_expenses.validator_fee_allocation_native as validator_fee_allocation_native

    -- Financial Statements
    , revenues_expenses.protocol_revenue_native as revenue_native
    , token_incentives_cte.token_incentives_native as token_incentives_native
    , token_incentives_cte.token_incentives_native as total_expenses_native
    , revenues_expenses.protocol_revenue_native - token_incentives_cte.token_incentives_native as earnings_native

    --Treasury Metrics
    , treasury_cte.treasury_value_native as treasury_native
    , net_treasury_cte.net_treasury_value_native as net_treasury_native
    , treasury_native_cte.treasury_native as own_token_treasury_native

FROM fees
FULL JOIN revenues_expenses USING (date, token)
FULL JOIN treasury_cte USING(date, token)
FULL JOIN treasury_native_cte USING(date, token)
FULL JOIN net_treasury_cte USING(date, token)
FULL JOIN token_incentives_cte USING(date, token)
LEFT JOIN steth_outstanding USING(date, token)
LEFT JOIN eth_deposited USING(date, token)
WHERE date < to_date(sysdate())