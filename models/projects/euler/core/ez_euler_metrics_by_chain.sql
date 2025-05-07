{{
    config(
        snowflake_warehouse="EULER", 
        materialized="table",
        database="EULER",
        schema="core",
        alias="ez_metrics_by_chain"
    )
}}

with lending_metrics as (
    select
        date
        , chain
        , sum(supplied_amount_cumulative) as lending_deposits
        , sum(borrow_amount_cumulative) as lending_loans
        , sum(supplied_amount_cumulative - borrow_amount_cumulative) as tvl
    from {{ ref("fact_euler_borrow_and_lending_metrics_by_chain") }}
    group by 1, 2
)

select
    lm.date
    , lm.chain
    , lm.lending_deposits
    , lm.lending_loans 
    , lm.tvl
from lending_metrics lm
