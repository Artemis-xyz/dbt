{{ config(
    materialized="table",
    snowflake_warehouse="METAPLEX"
) }}

WITH daily_mints AS (
    SELECT 
        event_date,
        COUNT(DISTINCT mint) AS daily_mints,
        COUNT_IF(program_id = 'BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY') AS daily_bubblegum_mints,
        COUNT_IF(program_id = 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s') AS daily_token_metadata_mints,
        COUNT_IF(program_id IN (
            'CndyV3LdqHUfDLmE5naZjVN8rBZz4tqhdefbAnjHG3JR', 
            'cndy3Z4yapfJBmL3ShUp5exZKqR3z33thTzeNMm2gRZ', 
            'cndyAnrLdpjq1Ssp1z8xxDsB8dxe7u4HL5Nxi2K5WXZ')) AS daily_candymachine_mints
    FROM 
        {{ ref('fact_metaplex_assets_minted') }}
    GROUP BY 
        event_date
)

SELECT 
    event_date,
    daily_mints,
    SUM(daily_mints) OVER (ORDER BY event_date) AS cumulative_mints,
    daily_bubblegum_mints,
    daily_token_metadata_mints,
    daily_candymachine_mints,
    SUM(daily_bubblegum_mints) OVER (ORDER BY event_date) AS cumulative_bubblegum_mints,
    SUM(daily_token_metadata_mints) OVER (ORDER BY event_date) AS cumulative_token_metadata_mints,
    SUM(daily_candymachine_mints) OVER (ORDER BY event_date) AS cumulative_candymachine_mints
FROM 
    daily_mints
ORDER BY 
    event_date;
