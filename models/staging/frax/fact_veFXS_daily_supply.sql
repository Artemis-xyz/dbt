{{ config(materialized="table") }}

WITH daily_supply_data AS (
    SELECT
        date,
        max(total_supply) as total_supply
    FROM {{ ref("fact_veFXS_raw_supply") }}
    GROUP BY date
),

-- Create a date spine from min date to current date
date_spine AS (
    SELECT
        ds.date
    FROM {{ ref('dim_date_spine') }} ds
    WHERE ds.date BETWEEN (SELECT MIN(date) FROM daily_supply_data) AND TO_DATE(SYSDATE())
),

-- Join the date spine with your data and apply forward fill
forward_filled_data AS (
    SELECT
        ds.date,
        LAST_VALUE(dsd.total_supply IGNORE NULLS) OVER (
            ORDER BY ds.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS total_supply
    FROM date_spine ds
    LEFT JOIN daily_supply_data dsd
        ON ds.date = dsd.date
)

-- Select the final result
SELECT 
    date,
    total_supply as circulating_supply
FROM forward_filled_data
ORDER BY date