{{
    config(
        materialized="table",
        snowflake_warehouse="STORY",
        database="story",
        schema="core",
        alias="ez_metrics",
    )
}}


WITH issued_supply_metrics AS (
    SELECT 
        date,
        total_supply + cumulative_ip_burned AS max_supply_native,
        total_supply AS total_supply_native,
        total_ip_burned AS native_burns,
        revenue as revenue,
        issued_supply AS issued_supply_native,
        circulating_supply AS circulating_supply_native
    FROM {{ ref('fact_story_issued_supply_and_float') }}
)

SELECT
    f.date,
    f.txns,
    f.daa AS dau,
    f.fees_native,
    f.fees,
    i.revenue,
    i.max_supply_native,
    i.total_supply_native,
    i.native_burns,
    i.issued_supply_native,
    i.circulating_supply_native
FROM {{ ref("fact_story_fundamental_metrics") }} f
LEFT JOIN issued_supply_metrics i
    ON f.date = i.date
WHERE f.date < TO_DATE(SYSDATE())

