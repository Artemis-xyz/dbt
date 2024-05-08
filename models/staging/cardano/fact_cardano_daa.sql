{{ config(materialized="view") }}
WITH cardano_data AS (
    SELECT
        parse_json(source_json) AS data
    FROM
        {{ source("PROD_LANDING", "raw_cardano_daa_partitioned") }}
)
SELECT
    date(value[0]) as date,
    F.value[1]::INT AS daa,
    F.value AS source,
    'cardano' AS chain
FROM
    cardano_data,
    LATERAL FLATTEN(input => data:data:values) AS F
ORDER BY DATE DESC