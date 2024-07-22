SELECT
    * EXCLUDE block_date,
    DATE_TRUNC('day', TO_TIMESTAMP_NTZ(block_date)) AS date
FROM {{ source('PROD_LANDING', 'injective_bridge_flows') }}