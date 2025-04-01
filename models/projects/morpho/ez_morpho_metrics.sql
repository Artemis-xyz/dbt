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
     from {{ ref("fact_morpho_deposits_loans") }}
     group by 1
 ),
 fees as (
     select
         date
         , sum(interest_usd) as fees
     from {{ ref("fact_morpho_fees") }}
     group by 1
 ),
 cumulative_metrics as (
     select
         d.date
         , sum(d.borrow_amount_usd) over (order by d.date rows between unbounded preceding and current row) as borrows
         , sum(d.supply_amount_usd) over (order by d.date rows between unbounded preceding and current row) as supplies
         , sum(d.deposit_amount_usd) over (order by d.date rows between unbounded preceding and current row) as deposits
         , fees
         , sum(f.fees) over (order by d.date rows between unbounded preceding and current row) as fees_cumulative
         , deposits - borrows as tvl
     from deposits d
     left join fees f on d.date = f.date
 )
 select
     date
     , borrows as daily_borrows_usd
     , supplies as total_available_supply
     , deposits as daily_supply_usd
     , tvl
     , fees
 from cumulative_metrics 