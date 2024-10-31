{{ config(
    materialized= "table",
    snowflake_warehouse="METAPLEX"
) }}

WITH metaplex_revenue AS (
    SELECT
        DATE(block_timestamp) AS date,
        COUNT(DISTINCT tx_id) * 0.005 AS protocol_revenue
    FROM {{ ref('fact_filtered_metaplex_solana_events') }}
    WHERE program_id = 'MPL4o4wMzndgh8T1NVDxELQCj5UQfYTYEkabX3wNKtb'
    GROUP BY 1

    UNION ALL

    SELECT
        DATE(block_timestamp) AS date,
        COUNT(DISTINCT tx_id) * 0.01 AS protocol_revenue
    FROM 
        {{ ref('fact_filtered_metaplex_solana_instructions') }}
    WHERE DATE(block_timestamp) >= '2023-06-01'
        AND decoded_instruction:name::STRING IN ('CreateMetadataAccountV3', 'Create')
        AND program_id IN (
            SELECT 'CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d' -- Core
            UNION ALL
            SELECT 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'  -- Token Metadata
        )
    GROUP BY 1

    UNION ALL

    SELECT
        DATE(block_timestamp) AS date,
        COUNT(DISTINCT tx_id) * 0.0015 AS protocol_revenue
    FROM 
        {{ ref('fact_filtered_metaplex_solana_instructions') }}
    WHERE 
        decoded_instruction:name::STRING IN ('CreateV1', 'CreateV2')
        AND program_id IN (
            SELECT 'CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d' -- Core
            UNION ALL
            SELECT 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'  -- Token Metadata
        )
    GROUP BY 1
)
, prices AS (
        SELECT  
        date(hour) AS date,
        symbol,
        avg(price) as price
    FROM
        {{source('SOLANA_FLIPSIDE_PRICE', 'ez_prices_hourly')}}
    WHERE is_native = TRUE
    GROUP BY 1, 2
)

SELECT
    r.date,
    p.symbol,
    r.protocol_revenue as revenue_native,
    r.protocol_revenue * p.price AS revenue_usd,
    SUM(r.protocol_revenue) OVER (ORDER BY r.date DESC) AS cumulative_revenue
FROM 
    metaplex_revenue r
LEFT JOIN
    prices p
    ON r.date = p.date
ORDER BY 
    r.date DESC
