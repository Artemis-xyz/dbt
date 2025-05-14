{{
    config(
        materialized="table",
        snowflake_warehouse = 'MORPHO',
        database = 'MORPHO',
        schema = 'core',
        alias = 'ez_metrics'
    )
 }}

with morpho_data as (
    select
        date
        , sum(dau) as dau
        , sum(txns) as txns
        , sum(borrow_amount_usd) as borrow_amount_usd
        , sum(supply_amount_usd) as supply_amount_usd
        , sum(supply_amount_usd) + sum(collat_amount_usd) as deposit_amount_usd
        , sum(fees_usd) as fees
    from {{ ref("fact_morpho_data") }}
    group by 1
)

, cumulative_metrics as (
    select
        d.date
        , d.dau
        , d.txns
        , sum(d.borrow_amount_usd) over (order by d.date rows between unbounded preceding and current row) as borrows
        , sum(d.supply_amount_usd) over (order by d.date rows between unbounded preceding and current row) as supplies
        , sum(d.deposit_amount_usd) over (order by d.date rows between unbounded preceding and current row) as deposits
        , fees
        , sum(fees) over (order by d.date rows between unbounded preceding and current row) as fees_cumulative
        , deposits - borrows as tvl
    from morpho_data d
 )

, morpho_market_data as (
    {{ get_coingecko_metrics('morpho') }}
)

, morpho_supply_data as (
    select
        date
        , premine_unlocks_native
        , net_supply_change_native
        , circulating_supply_native
    from {{ ref("fact_morpho_supply_data") }}
)

select
    date
    , dau
    , txns
    , borrows as daily_borrows_usd
    , supplies as total_available_supply
    , deposits as daily_supply_usd
    , fees

    -- Standardized metrics
    , dau as lending_dau
    , txns as lending_txns
    , borrows as lending_loans
    , supplies as lending_loan_capacity
    , deposits as lending_deposits
    , tvl
    
    -- Cash Flow Metrics (Interest goes to Liquidity Suppliers (Lenders) + Vaults Performance Fees)
    , fees as lending_interest_fees
    , lending_interest_fees as ecosystem_revenue
    
    -- Supply Metrics
    , msd.premine_unlocks_native
    , msd.net_supply_change_native
    , msd.circulating_supply_native

    -- Market Metrics
    , mdd.price
    , mdd.market_cap
    , mdd.fdmc
    , mdd.token_turnover_circulating
    , mdd.token_turnover_fdv
    , mdd.token_volume
from cumulative_metrics
left join morpho_market_data mdd using (date)
left join morpho_supply_data msd using (date)