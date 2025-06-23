{{
    config(
        materialized="table",
        snowflake_warehouse="EIGENLAYER",
        database="EIGENLAYER",
        schema="raw",
        alias="fact_eigenlayer_avs_and_operator_counts",
    )
}}

WITH AVSEvents AS (
    SELECT
        block_timestamp,
        DATE_TRUNC('day', block_timestamp) AS date,
        decoded_log:avs::STRING AS avs,
        decoded_log:operator::STRING AS operator,
        decoded_log:status::STRING AS status
    FROM {{ source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs') }}
    WHERE contract_address = '0x135dda560e946695d6f155dacafc6f1f25c1f5af'
    AND event_name = 'OperatorAVSRegistrationStatusUpdated'
),

-- Create a complete date spine
DateSpine AS (
    SELECT DISTINCT
        date
    FROM {{ ref('dim_date_spine') }}
    WHERE date > (SELECT MIN(date) FROM AVSEvents)
    AND date < CURRENT_DATE()
    ORDER BY date
),

-- Get all operator-AVS pairs
OperatorAVSPairs AS (
    SELECT DISTINCT operator, avs 
    FROM AVSEvents
),

-- Create all combinations of dates and operator-AVS pairs
DatePairCombinations AS (
    SELECT 
        d.date,
        p.operator,
        p.avs
    FROM DateSpine d
    CROSS JOIN OperatorAVSPairs p
),

-- Join with events and find the status as of each date
HistoricalStatus AS (
    SELECT
        c.date,
        c.operator,
        c.avs,
        e.block_timestamp,
        e.status
    FROM DatePairCombinations c
    LEFT JOIN AVSEvents e
        ON e.operator = c.operator
        AND e.avs = c.avs
        AND e.block_timestamp <= DATEADD('day', 1, c.date) - INTERVAL '1 second'
),

-- Get the latest status for each date and operator-AVS pair
LatestStatus AS (
    SELECT
        date,
        operator,
        avs,
        FIRST_VALUE(status) OVER (
            PARTITION BY date, operator, avs
            ORDER BY block_timestamp DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS current_status
    FROM HistoricalStatus
),

-- Remove duplicates
DeduplicatedStatus AS (
    SELECT DISTINCT
        date,
        operator,
        avs,
        current_status
    FROM LatestStatus
)

-- Count active relationships each day
SELECT
    date,
    COUNT(DISTINCT CASE WHEN current_status = '1' THEN operator END) AS active_operators,
    COUNT(DISTINCT CASE WHEN current_status = '1' THEN avs END) AS active_avs
FROM DeduplicatedStatus
GROUP BY date
ORDER BY date