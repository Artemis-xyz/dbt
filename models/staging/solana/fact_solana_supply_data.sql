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
    FROM {{ ref('fact_solana_vesting_schedule') }} v
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
    , sum(IFF(fundamental_usage.date < '2025-02-13', fees_native * .5, (base_fee_native + vote_tx_fee_native) * .5)) over (order by base.date ASC) as burns_cumulative
    , 500e6 + coalesce(cumulative_issuance,0) - coalesce(burns_cumulative,0) as total_supply
    , 52300000 as estimated_foundation_holdings
    , 500e6 + coalesce(cumulative_issuance,0) - 52300000 - coalesce(burns_cumulative,0) as issued_supply
    , coalesce(cumulative_unlocked,0) + coalesce(cumulative_issuance,0) - coalesce(burns_cumulative,0) - 52300000 as circulating_supply
FROM base 
LEFT JOIN {{ ref('fact_solana_issuance_silver') }} USING(date)
LEFT JOIN {{ ref('ez_solana_metrics') }} USING(date)