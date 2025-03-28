{{
    config(
        materialized="table",
        snowflake_warehouse = 'MORPHO',
        database = 'MORPHO',
        schema = 'core',
        alias = 'ez_metrics'
    )
}}

with deposits as (
    select
        date
        , sum(borrow_amount_usd) as borrow_amount_usd
        , sum(supply_amount_usd) as supply_amount_usd
        , sum(supply_amount_usd) + sum(collat_amount_usd) as deposit_amount_usd
        , 'morpho' as app
        , 'DeFi' as category
    from {{ ref("fact_morpho_deposits_loans") }}
    group by 1
),
cumulative_metrics as (
    select
        date
        , sum(borrow_amount_usd) over (order by date rows between unbounded preceding and current row) as borrows
        , sum(supply_amount_usd) over (order by date rows between unbounded preceding and current row) as supplies
        , sum(deposit_amount_usd) over (order by date rows between unbounded preceding and current row) as deposits
        , deposits - borrows as tvl
    from deposits
)
select
    date
    , borrows
    , supplies
    , deposits
    , tvl
from cumulative_metrics