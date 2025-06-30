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
    WHERE category <> 'Solana Foundation'
    GROUP BY 1
    
)
, one_time_burn as (
    SELECT
        '2020-05-31' as date,
        11365067 as sol_one_time_burn
)
SELECT
    b.date
    , b.amount_unlocked
    , 500e6 - b.amount_unlocked as unvested_tokens
    , b.cumulative_unlocked
    , i.issuance
    , 500e6 as initial_supply
    , sum(i.issuance) OVER (ORDER BY b.date ASC) as cumulative_issuance
    , sum(IFF(f.date < '2025-02-13', (f.gas + f.vote_tx_fee_native) * .5, (f.base_fee_native + f.vote_tx_fee_native) * .5) ) over (order by b.date ASC)  as burns_cumulative
    , SUM(coalesce(otb.sol_one_time_burn, 0) ) OVER (ORDER BY b.date ASC) as one_time_burns_cumulative
    , 500e6 + coalesce(cumulative_issuance,0) - coalesce(burns_cumulative,0) as total_supply
    , 52300000 as estimated_foundation_holdings
    , 500e6 + coalesce(cumulative_issuance,0) - 52300000 - coalesce(burns_cumulative,0) - coalesce(one_time_burns_cumulative, 0) as issued_supply
    , 447650000.00000000048 - cumulative_unlocked as unvested_insider_tokens
    , initial_supply - unvested_insider_tokens + coalesce(cumulative_issuance,0) - coalesce(burns_cumulative,0) - 52300000 - coalesce(one_time_burns_cumulative, 0) as circulating_supply
FROM base b
LEFT JOIN  fact_solana_issuance_silver   i USING(date)      
LEFT JOIN  fact_solana_fundamental_data f USING(date)
LEFT JOIN one_time_burn otb USING(date)