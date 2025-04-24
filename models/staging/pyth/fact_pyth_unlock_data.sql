{{
    config(
        materialized='table',
        unique_key='date',
        snowflake_warehouse='PYTH',
    )
}}

SELECT date, premine_unlocks_native
FROM (
    VALUES 
        ('2027-04-14', 2125000000),
        ('2026-04-19', 2125000000),
        ('2025-04-24', 2125000000),
        ('2024-04-29', 2125000000),
        ('2024-02-07', 100000000),
        ('2023-11-20', 255000000),
        ('2023-11-01', 1500000000)
) as s (date, premine_unlocks_native)