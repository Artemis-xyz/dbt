{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="core",
        alias="ez_metrics_by_chain"
    )
}}

WITH
    fees_revenue_expenses AS (
        SELECT
            date,
            stability_fees,
            trading_fees,
            fees,
            primary_revenue,
            liquidation_revenue,
            trading_revenue,
            other_revenue,
            protocol_revenue,
            token_incentives,
            direct_expenses,
            operating_expenses,
            total_expenses
        FROM {{ ref('fact_maker_fees_revenue_expenses') }}
    )
    , treasury_usd AS (
        SELECT date, treasury_usd FROM {{ ref('fact_treasury_usd') }}
    )
    , treasury_native AS (
        SELECT date, sum(amount_native) as treasury_native FROM {{ ref('fact_treasury_mkr') }} group by 1
    )
    , net_treasury AS (
        SELECT date, net_treasury_usd FROM {{ ref('fact_net_treasury_usd') }}
    )
    , tvl_metrics AS (
        SELECT date, sum(balance) as net_deposit, sum(balance) as tvl FROM {{ ref('fact_maker_tvl_by_address_balance') }}
        group by date
    )
    , outstanding_supply AS (
        SELECT date, outstanding_supply FROM {{ ref('fact_dai_supply') }}
    )
    , token_turnover_metrics as (
        select
            date
            , token_turnover_circulating
            , token_turnover_fdv
            , token_volume
        from {{ ref("fact_maker_fdv_and_turnover")}}
    )
    , price_data as ({{ get_coingecko_metrics("maker") }})
    , token_holder_data as (
        select
            date
            , tokenholder_count
        from {{ ref("fact_mkr_tokenholder_count")}}
    )


select
    date
    , 'ethereum' as chain
    , COALESCE(primary_revenue, 0) AS primary_revenue
    , COALESCE(other_revenue, 0) AS other_revenue
    
    , COALESCE(treasury_usd, 0) AS treasury_usd
    , COALESCE(net_treasury_usd, 0) AS net_treasury_usd
    , COALESCE(net_deposit, 0) AS net_deposits
    , COALESCE(outstanding_supply, 0) AS outstanding_supply
    , COALESCE(tokenholder_count, 0) AS tokenholder_count

    -- Standardized metrics
    , 'Maker' as app
    , 'DeFi' as category
    , COALESCE(stability_fees,0) as stability_fees
    , COALESCE(trading_fees, 0) AS trading_fees
    , COALESCE(fees, 0) AS fees
    , COALESCE(protocol_revenue, 0) AS treasury_fee_allocation

    , COALESCE(protocol_revenue, 0) AS revenue
    , COALESCE(token_incentives, 0) AS token_incentives
    , COALESCE(operating_expenses, 0) AS operating_expenses
    , COALESCE(direct_expenses, 0) AS direct_expenses
    , COALESCE(total_expenses, 0) AS total_expenses
    , COALESCE(revenue - total_expenses, 0) AS earnings

    , COALESCE(treasury_usd, 0) AS treasury
    , COALESCE(treasury_native, 0) AS treasury_native

    , COALESCE(net_deposit, 0) AS lending_deposits
    , COALESCE(outstanding_supply, 0) AS lending_loans

    , COALESCE(tvl, 0) AS tvl
    , COALESCE(price, 0) AS price
    , COALESCE(fdmc, 0) AS fdmc
    , COALESCE(market_cap, 0) AS market_cap
    , COALESCE(token_volume, 0) AS token_volume
    , COALESCE(token_turnover_fdv, 0) AS token_turnover_fdv
    , COALESCE(token_turnover_circulating, 0) AS token_turnover_circulating

   
FROM token_holder_data
left join treasury_usd using (date)
left join treasury_native using (date)
left join net_treasury using (date)
left join tvl_metrics using (date)
left join outstanding_supply using (date)
left join token_turnover_metrics using (date)
left join price_data using (date)
left join fees_revenue_expenses using (date)
where date < to_date(sysdate())