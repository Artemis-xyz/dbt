{{
    config(
        materialized = "table",
        snowflake_warehouse = "CHILIZ",
        database = "CHILIZ",
        schema = "core",
        alias = "ez_metrics"
    )
}}

with date_spine as(
    SELECT
        date
    FROM {{ ref('dim_date_spine') }}
    WHERE date between '2018-10-26' and to_date(sysdate())
)
, fees as (
    select
        date,
        fees_usd
    from {{ref("fact_chiliz_fees")}}
),
txns as (
    select
        date,
        txns
    from {{ref("fact_chiliz_txns")}}
)
, daus as (
    select
        date,
        dau
    from {{ref("fact_chiliz_dau")}}
    where dau < 170000 -- There is a DQ issue with the Chiliz dau data: 2 days with > 170k DAU while the rest of the data around those days is < 1k
)
, burns as (
    select
        date,
        burns_native,
        revenue
    from {{ref("fact_chiliz_burns")}}
)
, treasury as (
    select
        date,
        native_balance,
        native_balance_change,
        usd_balance,
        usd_balance_change
    from {{ref("fact_chiliz_treasury")}}
)
, price_data as ({{ get_coingecko_metrics("chiliz") }})
, supply_data as (
    select
        date,
        gross_emissions_native,
        circulating_supply_native
    from {{ref("fact_chiliz_supply")}}
)
select
    ds.date
    , dau
    , txns
    , fees_usd as fees
    , coalesce(revenue, 0) as revenue
    , coalesce(burns_native, 0) as burns_native
    , usd_balance as treasury_value
    , usd_balance_change as treasury_value_native_change
    
    -- Standardized Metrics
    
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    
    -- Chain Usage Metrics
    , txns AS chain_txns
    , dau AS chain_dau
    
    -- Cashflow metrics
    , fees AS chain_fees
    , fees_usd AS ecosystem_revenue
    , revenue AS burned_fee_allocation
    , burns_native AS burned_fee_allocation_native
    
    -- Protocol Metrics
    , usd_balance AS treasury
    , usd_balance_change AS treasury_native_change

    -- Supply metrics
    , gross_emissions_native
    , circulating_supply_native
from date_spine ds
left join fees using (date)
left join txns using (date)
left join daus using (date)
left join burns using (date)
left join treasury using (date)
left join price_data using (date)
left join supply_data using (date)