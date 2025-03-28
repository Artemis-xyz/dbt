{{
    config(
        materialized="table",
        snowflake_warehouse="GRASS",
    )
}}

WITH max_extraction AS (
    SELECT MAX(extraction_date) AS max_date
    FROM LANDING_DATABASE.PROD_LANDING.raw_grass_data_scraped
),
latest_data AS (
    SELECT PARSE_JSON(source_json):result:data AS data_array
    FROM LANDING_DATABASE.PROD_LANDING.raw_grass_data_scraped
    WHERE extraction_date = (SELECT max_date FROM max_extraction)
)
, flattened_data AS (
    SELECT
        f.value:date::date AS date,
        f.value:dailyDataCollected::number AS data_collected_tb
    FROM latest_data,
    LATERAL FLATTEN(input => latest_data.data_array) AS f
    )
SELECT date, data_collected_tb
FROM flattened_data
WHERE date < TO_DATE(SYSDATE())
ORDER BY date DESC