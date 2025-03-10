{{
    config(
        materialized="table",
        snowflake_warehouse="OPTIMISM",
        database="optimism",
        schema="raw",
        alias="fact_optimism_insider_unlocks",
    )
}}
WITH date_spine AS (
    SELECT date
    FROM pc_dbt_db.prod.dim_date_spine
    WHERE date BETWEEN '2022-05-31' AND '2030-12-31'
)
SELECT
    date,
    'Investor Unlocks' as event_type,
    CASE WHEN date between '2023-05-01' and '2024-04-01'
        THEN   352599107  / 12
        WHEN date between '2024-05-01' and '2025-04-01'
        THEN   183964751  / 12
        WHEN date between '2025-05-01' and '2026-04-01'
        THEN   183964751  / 12
        WHEN date between '2026-05-01' and '2027-04-01'
        THEN   15330287  / 12
    END as amount,
    'https://docs.google.com/spreadsheets/d/1qVMhLmmch3s6XSbiBe8hgD4ntMkPIOhc1WrhsYsQc7M/edit?gid=470961921#gid=470961921' as source
FROM date_spine
WHERE DATE_PART('day', date) = 1
AND date between '2023-05-01' and '2027-04-01'
UNION ALL
SELECT
    date,
    'Early Core Contributors Unlocks' as event_type,
    CASE WHEN date between '2023-05-01' and '2024-04-01'
        THEN 329543520 / 12
        WHEN date between '2024-05-01' and '2025-04-01'
        THEN   175040901 / 12
        WHEN date between '2025-05-01' and '2026-04-01'
        THEN   173301104 / 12
        WHEN date between '2026-05-01' and '2027-04-01'
        THEN     14508052 / 12
    END as amount,
    'https://docs.google.com/spreadsheets/d/1qVMhLmmch3s6XSbiBe8hgD4ntMkPIOhc1WrhsYsQc7M/edit?gid=470961921#gid=470961921' as source
FROM date_spine
WHERE DATE_PART('day', date) = 1
AND date between '2023-05-01' and '2027-04-01'