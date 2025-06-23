{{
    config(
        materialized="table",
        snowflake_warehouse="SOLANA",
    )
}}

with base as (
    SELECT 
        v.date
        , sum(amount_unlocked) as amount_unlocked
        , sum(cumulative_unlocked) as cumulative_unlocked
    FROM fact_solana_vesting_schedule v
    GROUP BY 1
    
)
SELECT
    base.date
    , amount_unlocked
    , 500e6 - amount_unlocked as unvested_tokens
    , cumulative_unlocked
    , issuance
    , 500e6 as initial_supply
    , sum(issuance) OVER (ORDER BY date ASC) as cumulative_issuance
    , sum(burned_fee_allocation_native) over (order by base.date ASC) as burns_cumulative
    , 500e6 + coalesce(cumulative_issuance,0) - coalesce(burns_cumulative,0) as total_supply
    , 52300000 as estimated_foundation_holdings
    , 500e6 + coalesce(cumulative_issuance,0) - 52300000 - coalesce(burns_cumulative,0) as issued_supply
    , coalesce(cumulative_unlocked,0) + coalesce(cumulative_issuance,0) - coalesce(burns_cumulative,0) - 52300000 as circulating_supply
FROM base 
LEFT JOIN fact_solana_issuance_silver USING(date)
LEFT JOIN solana.prod_core.ez_metrics USING(date)