{{config(
    materialized = 'table',
    snowflake_warehouse = 'ALGORAND'
)}}

WITH date_spine AS (
    SELECT date
    FROM {{ ref('dim_date_spine') }}
    WHERE date >= '2019-06-01' AND date < TO_DATE(SYSDATE())
)

, foundation_balances AS (
    SELECT
        date
        , SUM(balance) AS foundation_owned_balance
    FROM {{ ref('fact_algorand_foundation_balances') }}
    GROUP BY 1
)
, monthly_unlocks AS (
    SELECT
        date,
        ROUND(
            SUM(end_user_grant)
            + SUM(foundation)
            + SUM(node_running_grant)
            + SUM(participation_rewards)
            + SUM(public_sale)
            + SUM(team_and_investors)
            , 0
        ) AS vested_supply,
        10000000000 
        - ROUND(
            SUM(end_user_grant)
            + SUM(foundation)
            + SUM(node_running_grant)
            + SUM(participation_rewards)
            + SUM(public_sale)
            + SUM(team_and_investors)
            , 0
        ) AS unvested_supply
    FROM {{ source('MANUAL_STATIC_TABLES', 'algorand_monthly_unlocks') }}
    GROUP BY date
)

, backfilled_supply AS (
    SELECT
    ds.date,
    LAST_VALUE(vested_supply IGNORE NULLS) OVER (
        ORDER BY ds.date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS vested_supply, 
    LAST_VALUE(unvested_supply IGNORE NULLS) OVER (
        ORDER BY ds.date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS unvested_supply, 
    FROM date_spine AS ds
    LEFT JOIN monthly_unlocks
        ON ds.date = monthly_unlocks.date
    ORDER BY ds.date DESC
)

SELECT 
    date, 
    unvested_supply, 
    vested_supply - LAG(vested_supply) OVER (ORDER BY date) AS premine_unlocks
FROM backfilled_supply
ORDER BY date DESC