{{
    config(
        materialized="table",
        snowflake_warehouse="ORCA",
    )
}}

with locked as (
    SELECT 
        date
        -- , address
        , sum(balance_native) as locked_tokens
    FROM {{ ref("fact_orca_treasury_balance") }}
    GROUP BY 1
    -- , 2
    ORDER BY 1 DESC
)
SELECT
    date
    , locked_tokens - lag(locked_tokens) over (order by date asc) as premine_unlocks_native
    , premine_unlocks_native as net_change
    , case when date < '2025-04-16'
        then 99500000 - locked_tokens
        else 99500000 - 25000000 - locked_tokens
    end as circulating_supply_native
FROM locked