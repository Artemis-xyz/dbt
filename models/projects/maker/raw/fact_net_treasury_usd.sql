{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_net_treasury_usd"
    )
}}

with agg as(
    SELECT
        date,
        surplus as amount_usd
    FROM
        {{ ref('fact_system_surplus_dai') }}
    UNION ALL
    SELECT
        date,
        amount_usd
    FROM
        {{ ref('fact_treasury_lp_balances') }}
)
SELECT
    date,
    SUM(amount_usd) AS net_treasury_usd
FROM
    agg
GROUP BY
    1
ORDER BY
    1 DESC