{{
    config(
        materialized="incremental",
        snowflake_warehouse="PENDLE",
        database="pendle",
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

with
    swap_fees as (
        SELECT
            date
            , SUM(fees) as swap_fees
            , SUM(supply_side_fees) as supply_side_fees
            , SUM(revenue) as swap_revenue
            , SUM(volume) as swap_volume
        FROM
        {{ ref('fact_pendle_trades') }}
        GROUP BY 1
    )
    , yield_fees as (
        SELECT
            date
            , SUM(fees) as yield_revenue
        FROM
            {{ ref('fact_pendle_yield_fees') }}
        GROUP BY 1
    )
    , daus_txns as (
        SELECT
            date
            , SUM(daus) as daus
            , SUM(daily_txns) as daily_txns
        FROM
            {{ ref('fact_pendle_daus_txns') }}
        GROUP BY 1
    )
    , token_incentives_cte as (
        SELECT
            date
            , SUM(token_incentives) as token_incentives
            , SUM(token_incentives_native) as token_incentives_native
        FROM
            {{ref('fact_pendle_token_incentives_by_chain')}}
        GROUP BY 1
    )
    , tvl as (
        SELECT
            date
            , SUM(tvl_usd) as tvl
            , SUM(tvl_usd) as net_deposits
        FROM
            {{ref('fact_pendle_tvl_by_token_and_chain')}}
        GROUP BY 1
    )
    , treasury_value_cte as (
        select
            date,
            sum(balance) as treasury_value
        from {{ref('fact_pendle_treasury')}}
        group by 1
    )
    , net_treasury_value_cte as (
        select
            date,
            sum(balance) as net_treasury_value
        from {{ref('fact_pendle_treasury')}}
        where token <> 'PENDLE'
        group by 1
    )
    , treasury_value_native_cte as (
        select
            date,
            sum(balance_native) as treasury_value_native,
            sum(balance) as native_treasury_value
        from {{ref('fact_pendle_treasury')}}
        where token = 'PENDLE'
        group by 1
    )
    , price_data_cte as(
        {{ get_coingecko_metrics('pendle') }}
    )
    , tokenholder_count as (
        select * 
        from {{ref('fact_pendle_token_holders')}}
    )
    , supply_data as (
        SELECT
            date,
            emissions_native,
            unlocks_native,
            pendle_locked,
            total_supply_native,
            issued_supply_native,
            circulating_supply_native
        FROM {{ ref('fact_pendle_supply_data')}}
    )
SELECT
    p.date
    , 'pendle' as artemis_id

    -- Standardized Metrics

    -- Market Metrics
    , p.price
    , p.fdmc
    , p.market_cap
    , p.token_volume

    --Usage Metrics
    , d.daus as spot_dau
    , d.daus as dau
    , d.daily_txns as spot_txns
    , d.daily_txns as txns
    , f.swap_volume as spot_volume
    , t.tvl as tvl
    , {{ daily_pct_change('t.tvl') }} as tvl_pct_change

    -- Fee Metrics
    , coalesce(yf.yield_revenue, 0) as yield_fees
    , coalesce(f.swap_fees, 0) as spot_fees
    , coalesce(f.swap_fees, 0) + coalesce(yf.yield_revenue, 0) as fees
    , coalesce(f.swap_revenue, 0) + coalesce(yf.yield_revenue, 0) as staking_fee_allocation
    , f.supply_side_fees as lp_fee_allocation

    -- Financial Statement Metrics
    , 0 as revenue
    , f.swap_revenue + yf.yield_revenue as staking_revenue
    , coalesce(ti.token_incentives, 0) as token_incentives
    , revenue - token_incentives as earnings

    -- Treasury Metrics
    , tv.treasury_value as treasury
    , tn.native_treasury_value as own_token_treasury
    , tn.treasury_value_native as own_token_treasury_native
    , nt.net_treasury_value as net_treasury

    -- Supply Metrics
    , emissions_native
    , unlocks_native as premine_unlocks_native
    , pendle_locked as locked_supply_native
    , total_supply_native
    , issued_supply_native
    , circulating_supply_native

    -- Other Metrics
    , coalesce(ti.token_incentives, 0) as gross_emissions
    , coalesce(ti.token_incentives_native, 0) as gross_emissions_native

    , p.token_turnover_fdv
    , p.token_turnover_circulating
    , tc.token_holder_count

    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on

FROM price_data_cte p
LEFT JOIN swap_fees f using(date)
LEFT JOIN yield_fees yf using(date)
LEFT JOIN daus_txns d using(date)
LEFT JOIN token_incentives_cte ti using(date)
LEFT JOIN tvl t USING (date)
LEFT JOIN treasury_value_cte tv USING (date)
LEFT JOIN net_treasury_value_cte nt USING (date)
LEFT JOIN treasury_value_native_cte tn USING (date) 
LEFT JOIN tokenholder_count tc using(date) 
LEFT JOIN supply_data sd using(date)
where true
{{ ez_metrics_incremental('p.date', backfill_date) }}
and p.date < to_date(sysdate())
