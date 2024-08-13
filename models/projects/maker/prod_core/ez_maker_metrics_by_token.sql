{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="core",
        alias="ez_metrics_by_token"
    )
}}

WITH
    fees_revenue_expenses AS (
        SELECT
            date,
            token,
            stability_fees,
            trading_fees,
            fees,
            primary_revenue,
            other_revenue,
            protocol_revenue,
            token_incentives,
            direct_expenses,
            operating_expenses,
            total_expenses
        FROM {{ ref('fact_maker_fees_revenue_expenses_by_token') }}
    )
    , treasury AS (
            SELECT
                date,
                token,
                surplus as treasury
            FROM
                {{ ref('fact_system_surplus_dai') }}
            UNION ALL
            SELECT
                date,
                token,
                amount_mkr as treasury
            FROM
                {{ ref('fact_treasury_mkr') }}
            UNION ALL
            SELECT
                date,
                token,
                amount_native as treasury
            FROM
                {{ ref('fact_treasury_lp_balances') }}
    )
    , treasury_native AS (
        SELECT date, token, amount_mkr as treasury_native FROM {{ ref('fact_treasury_mkr') }}
    )
    , net_treasury AS (
            SELECT
                date,
                'DAI' as token,
                surplus as net_treasury
            FROM
                {{ ref('fact_system_surplus_dai') }}
            UNION ALL
            SELECT
                date,
                token,
                amount_native as net_treasury
            FROM
                {{ ref('fact_treasury_lp_balances') }}
    )
    , tvl_metrics AS (
        SELECT
            date,
            symbol as token,
            total_amount_native as tvl
        FROM {{ ref('fact_maker_tvl_by_asset') }}
    )
    , outstanding_supply AS (
        SELECT date, 'DAI' as token, outstanding_supply FROM {{ ref('fact_dai_supply') }}
    )


select
    date
    , token
    , COALESCE(stability_fees,0) as stability_fees
    , COALESCE(trading_fees, 0) AS trading_fees
    , COALESCE(fees, 0) AS fees
    , COALESCE(primary_revenue, 0) AS primary_revenue
    , COALESCE(other_revenue, 0) AS other_revenue
    , COALESCE(protocol_revenue, 0) AS protocol_revenue
    , COALESCE(token_incentives, 0) AS token_incentives
    , COALESCE(operating_expenses, 0) AS operating_expenses
    , COALESCE(direct_expenses, 0) AS direct_expenses
    , COALESCE(total_expenses, 0) AS total_expenses
    , COALESCE(protocol_revenue - total_expenses, 0) AS protocol_earnings
    , COALESCE(treasury, 0) as treasury_value
    , COALESCE(treasury_native,0) as treasury_native
    , COALESCE(net_treasury, 0) as net_treasury
    , COALESCE(tvl, 0) as net_deposits
    , COALESCE(outstanding_supply,0) as outstanding_supply
    , COALESCE(tvl, 0) as tvl
FROM fees_revenue_expenses
full join treasury using (date, token)
full join treasury_native using (date, token)
full join net_treasury using (date, token)
full join tvl_metrics using (date, token)
full join outstanding_supply using (date, token)
where date < to_date(sysdate())