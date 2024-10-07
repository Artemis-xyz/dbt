{{
    config(
        materialized="table",
        snowflake_warehouse="outerlands"
    )
}}

WITH 
    monthly_weights AS (
        SELECT
            date,
            coingecko_id,
            combined_score AS weight,
            LEAD(date) OVER (PARTITION BY coingecko_id ORDER BY date) AS next_month_start
        FROM {{ref('fact_outerlands_asset_weights_for_month_start')}}
    )
    , daily_performance AS (
        SELECT
            d.date,
            d.coingecko_id,
            d.daily_percent_change,
            w.weight
        FROM {{ref('fact_outerlands_daily_asset_price_change')}} d
        JOIN monthly_weights w
            ON d.coingecko_id = w.coingecko_id
            AND d.date >= w.date
            AND (d.date < w.next_month_start OR w.next_month_start IS NULL)
    )
    , index_daily_performance AS (
        SELECT
            date,
            SUM(daily_percent_change * weight) / 100 AS index_daily_return
        FROM daily_performance
        GROUP BY date
    )
    , cumulative_performance AS (
        SELECT
            date,
            index_daily_return,
            EXP(SUM(LN(1 + COALESCE(index_daily_return, 0))) OVER (ORDER BY date)) AS cumulative_index_value,
            FIRST_VALUE(date) OVER (ORDER BY date) AS start_date
        FROM index_daily_performance
        WHERE date >= date('2013-05-01')
    )
SELECT
    cp.date,
    cp.index_daily_return,
    CASE 
        WHEN cp.date = cp.start_date THEN 1
        ELSE cp.cumulative_index_value
    END AS cumulative_index_value
FROM cumulative_performance cp
ORDER BY cp.date