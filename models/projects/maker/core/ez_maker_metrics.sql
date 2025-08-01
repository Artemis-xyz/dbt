{{
    config(
        materialized="incremental",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

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
        SELECT date, sum(amount_native) as own_token_treasury 
        FROM {{ ref('fact_treasury_mkr') }} 
        WHERE token in ('MKR', 'SKY')
        group by 1
    )
    , net_treasury AS (
        SELECT date, net_treasury_usd 
        FROM {{ ref('fact_net_treasury_usd') }}
    )
    , tvl_metrics AS (
        SELECT 
            date, 
            sum(balance) as tvl
        FROM {{ ref('fact_maker_tvl_by_address_balance') }} 
        GROUP BY 1
    )
    , outstanding_supply AS (
        SELECT date, outstanding_supply 
        FROM {{ ref('fact_dai_supply') }}
    )
    , token_turnover_metrics as (
        select
            date
            , token_turnover_circulating
            , token_turnover_fdv
            , token_volume
        from {{ ref("fact_maker_fdv_and_turnover")}}
    )
    , market_data as ({{ get_coingecko_metrics("maker") }})
    , token_holder_data as (
        select
            date
            , tokenholder_count
        from {{ ref("fact_mkr_tokenholder_count")}}
    )
    , token_supply as (
        select
            date
            , issued_supply_sky_less_converter as sky_issued_supply
            , circulating_supply_sky_less_converter as sky_circulating_supply
        from {{ ref("fact_maker_supply_data")}}
    )


select
    date
    , 'maker' as artemis_id

    -- Standardized metrics
    -- Market Metrics
    , COALESCE(market_data.price, 0) AS price
    , COALESCE(market_data.fdmc, 0) AS fdmc
    , COALESCE(market_data.market_cap, 0) AS market_cap
    , COALESCE(market_data.token_volume, 0) AS token_volume
    
    -- Usage Metrics
    , COALESCE(tvl, 0) AS tvl
    , COALESCE(tvl, 0) AS lending_deposits
    , COALESCE(outstanding_supply, 0) AS lending_loans

    -- Fees Metrics
    , COALESCE(fees.stability_fees,0) as lending_fees
    , COALESCE(fees.trading_fees, 0) AS trading_fees
    , COALESCE(fees.fees, 0) AS fees
    , COALESCE(fees.fees, 0) AS treasury_fee_allocation 

    -- Financial  Metrics
    , COALESCE(fees.protocol_revenue, 0) AS revenue
    , COALESCE(fees.token_incentives, 0) AS token_incentives
    , COALESCE(fees.operating_expenses, 0) AS operating_expenses
    , COALESCE(fees.direct_expenses, 0) AS direct_expenses
    , COALESCE(fees.total_expenses, 0) AS total_expenses
    , COALESCE(fees.protocol_revenue - fees.total_expenses, 0) AS earnings
    

    -- Treasury Metrics
    , COALESCE(treasury_usd, 0) AS treasury
    , COALESCE(own_token_treasury, 0) AS own_token_treasury

    -- Supply Metrics
    , COALESCE(sky_issued_supply, 0) AS issued_supply_native
    , COALESCE(sky_circulating_supply, 0) AS circulating_supply_native

    -- Token Turnover metrics
    , COALESCE(turnover.token_turnover_fdv, 0) AS token_turnover_fdv
    , COALESCE(turnover.token_turnover_circulating, 0) AS token_turnover_circulating

    , COALESCE(tokenholder_count, 0) AS tokenholder_count
    
    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
FROM token_holder_data
left join treasury_usd using (date)
left join treasury_native using (date)
left join net_treasury using (date)
left join tvl_metrics using (date)
left join outstanding_supply using (date)
left join token_turnover_metrics turnover using (date)
left join market_data using (date)
left join fees_revenue_expenses fees using (date)
left join token_supply using (date)
where true
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())