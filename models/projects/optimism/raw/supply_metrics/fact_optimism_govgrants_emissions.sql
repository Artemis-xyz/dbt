{{
    config(
        materialized="table",
        snowflake_warehouse="OPTIMISM",
        database="optimism",
        schema="raw",
        alias="fact_optimism_govgrants_emissions",
    )
}}

with source_url AS (
    SELECT 'https://docs.google.com/spreadsheets/d/1Ul8iMTsOFUKUmqz6MK0zpgt8Ki8tFtoWKGlwXj-Op34/edit?gid=362085946#gid=362085946' AS source
)
SELECT '2022-07-08' AS date, 'GovGrants' AS event_type, 36000000.00 AS amount, source
FROM source_url
UNION ALL SELECT '2022-08-08', 'GovGrants', 2430770.00, source FROM source_url
UNION ALL SELECT '2022-08-26', 'GovGrants', 1200000.00, source FROM source_url
UNION ALL SELECT '2022-10-19', 'GovGrants', 3000000.00, source FROM source_url
UNION ALL SELECT '2022-11-04', 'GovGrants', 1754764.00, source FROM source_url
UNION ALL SELECT '2022-12-06', 'GovGrants', 3839069.00, source FROM source_url
UNION ALL SELECT '2023-03-01', 'GovGrants', 7524828.00, source FROM source_url
UNION ALL SELECT '2023-04-01', 'GovGrants', 2460000.00, source FROM source_url
UNION ALL SELECT '2023-08-01', 'GovGrants', 2069500.00, source FROM source_url
UNION ALL SELECT '2023-12-06', 'GovGrants', 2487276.00, source FROM source_url
UNION ALL SELECT '2024-04-01', 'GovGrants', 2554000.00, source FROM source_url
UNION ALL SELECT '2024-06-01', 'GovGrants', 4576874.00, source FROM source_url
UNION ALL SELECT '2024-09-01', 'GovGrants', 769999.00, source FROM source_url
UNION ALL SELECT '2024-10-01', 'GovGrants', 2092564.00, source FROM source_url
UNION ALL SELECT '2024-11-01', 'GovGrants', 6471983.00, source FROM source_url
UNION ALL SELECT '2024-12-01', 'GovGrants', 1477478.00, source FROM source_url
UNION ALL SELECT '2025-01-01', 'GovGrants', 2890172.00, source FROM source_url
UNION ALL SELECT '2025-02-01', 'GovGrants', 2407094.00, source FROM source_url