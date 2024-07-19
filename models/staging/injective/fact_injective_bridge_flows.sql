SELECT
    * EXCLUDE date,
    DATEADD('day', 1, DATE_TRUNC('day', TO_TIMESTAMP_NTZ(date))) AS date
FROM {{ source('PROD_LANDING', 'injective_bridge_flows') }}