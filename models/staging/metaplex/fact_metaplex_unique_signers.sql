{{ config(
    materialized="table",
    snowflake_warehouse="METAPLEX"
) }}

WITH filtered_transactions AS (
    SELECT 
        t.tx_id,
        DATE_TRUNC('day', t.block_timestamp) AS day_date,
        ARRAY_AGG(DISTINCT TRIM(LOWER(unnested_signer.value::STRING))) AS signers
    FROM 
        solana_flipside.core.fact_transactions t
    INNER JOIN 
        solana_flipside.core.fact_events e 
        ON t.tx_id = e.tx_id
        AND e.succeeded = TRUE
        AND t.succeeded = TRUE
    INNER JOIN 
        {{ ref('fact_metaplex_programs') }} mp 
        ON e.program_id = mp.program_id
    , LATERAL FLATTEN(input => t.signers) AS unnested_signer
    GROUP BY 
        t.tx_id, day_date
),

date_range AS (
    SELECT 
        MIN(day_date) AS min_date,
        MAX(day_date) AS max_date
    FROM 
        filtered_transactions
),

date_spine AS (
    SELECT min_date AS day
    FROM date_range
    UNION ALL
    SELECT DATEADD(day, 1, day) AS day
    FROM date_spine
    WHERE day < (SELECT max_date FROM date_range)
),

daily_stats AS (
    SELECT 
        day_date AS day,
        COUNT(DISTINCT f.value) AS unique_signers
    FROM 
        filtered_transactions,
        LATERAL FLATTEN(input => signers) f
    GROUP BY 
        day_date
)

SELECT 
    d.day as date,
    COALESCE(m.unique_signers, 0) AS unique_signers
FROM 
    date_spine d
LEFT JOIN 
    daily_stats m 
ON 
    d.day = m.day
ORDER BY 
    d.day DESC
