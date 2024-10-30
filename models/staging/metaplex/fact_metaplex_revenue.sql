{{ config(
    materialized= "table",
    snowflake_warehouse="METAPLEX"
) }}

WITH metaplex_revenue AS (
    SELECT
        DATE(block_timestamp) AS block_date,
        COUNT(DISTINCT tx_id) * 0.005 AS protocol_revenue
    FROM {{ source('SOLANA_FLIPSIDE', 'fact_events') }}
    WHERE program_id = 'MPL4o4wMzndgh8T1NVDxELQCj5UQfYTYEkabX3wNKtb'
    GROUP BY block_date  

    UNION ALL

    SELECT
        DATE(block_timestamp) AS block_date,
        COUNT(DISTINCT tx_id) * 0.01 AS protocol_revenue
    FROM {{ source('SOLANA_FLIPSIDE', 'fact_decoded_instructions') }}
    WHERE DATE(block_timestamp) >= '2023-06-01'
        AND decoded_instruction:name::STRING IN ('CreateMetadataAccountV3', 'Create')
        AND program_id IN (
            SELECT 'CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d' -- Core
            UNION ALL
            SELECT 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'  -- Token Metadata
        )
    GROUP BY block_date

    UNION ALL

    SELECT
        DATE(block_timestamp) AS date,
        COUNT(DISTINCT tx_id) * 0.0015 AS protocol_revenue
    FROM {{ source('SOLANA_FLIPSIDE', 'fact_decoded_instructions') }}
    WHERE decoded_instruction:name::STRING IN ('CreateV1', 'CreateV2')
        AND program_id IN (
            SELECT 'CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d' -- Core
            UNION ALL
            SELECT 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'  -- Token Metadata
        )
    GROUP BY date
)

SELECT
    date,
    protocol_revenue,
    SUM(protocol_revenue) OVER (ORDER BY date DESC) AS cumulative_revenue
FROM 
    metaplex_revenue
ORDER BY 
    date DESC
