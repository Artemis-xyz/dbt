{{ config(materialized="view", snowflake_warehouse="CARDANO") }}
WITH cardano_data AS (
    SELECT 
        parse_json(source_json) AS data
    FROM 
        {{ source("PROD_LANDING", "raw_cardano_txns") }}
)
SELECT 
    date(value[0]) as date,
    F.value[1]::INT AS txns,
    F.value AS source, 
    'cardano' AS chain
FROM 
    cardano_data,
    LATERAL FLATTEN(input => data:data:values) AS F
ORDER BY DATE DESC