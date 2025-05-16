{{ config(materialized="table") }}

with all_extracted_values as (
    SELECT
        a.key as date,
        a.value:totalKm::number as total_km,
        a.value:totalUniqueKm::number as total_unique_km,
        extraction_date
    FROM {{ source("PROD_LANDING", "raw_hivemapper_KMs") }}
    , lateral flatten(input => source_json:stats:byDay) a
)
SELECT
    date,
    max_by(total_km, extraction_date) as total_km,
    max_by(total_unique_km, extraction_date) as total_unique_km
FROM all_extracted_values
GROUP BY 1