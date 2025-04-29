{{ config(materialized="table") }}


WITH max_data AS (
    SELECT
        source_json
    from {{ source("PROD_LANDING", "raw_jupiter_aggregator_volume") }}
    WHERE extraction_date = (SELECT MAX(extraction_date) FROM {{ source("PROD_LANDING", "raw_jupiter_aggregator_volume") }})
)
SELECT
    left(a.value:date, 10)::date as date,
    a.value:overall::number as overall,
    a.value:single::number as single
FROM
    max_data,
LATERAL FLATTEN(input => source_json) a