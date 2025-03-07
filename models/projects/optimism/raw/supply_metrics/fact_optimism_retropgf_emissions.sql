{{
    config(
        materialized="table",
        snowflake_warehouse="OPTIMISM",
        database="optimism",
        schema="raw",
        alias="fact_optimism_retropgf_emissions",
    )
}}

SELECT
    '2023-03-29' AS date,
    'RetroPGF Round 2' AS event_type,
    10 * 1e6 AS amount,
    'https://github.com/ethereum-optimism/community-hub/blob/main/pages/citizens-house/rounds/retropgf-2.mdx' AS source
UNION ALL
SELECT
    '2024-01-10' AS date, -- Estimate (announced as January 2024)
    'RetroPGF Round 3' AS event_type,
    30 * 1e6 AS amount,
    'https://github.com/ethereum-optimism/community-hub/blob/main/pages/citizens-house/rounds/retropgf-3.mdx' AS source
UNION ALL
SELECT
    '2024-07-16' AS date, -- Results & Grant delivery date
    'RetroPGF Round 4' AS event_type,
    10 * 1e6 AS amount,
    'https://github.com/ethereum-optimism/community-hub/blob/main/pages/citizens-house/rounds/retropgf-4.mdx' AS source
UNION ALL
SELECT
    '2024-10-21' AS date, -- Results & Grant delivery date
    'RetroPGF Round 5' AS event_type,
    8 * 1e6 AS amount,
    'https://github.com/ethereum-optimism/community-hub/blob/main/pages/citizens-house/rounds/retropgf-4.mdx' AS source
UNION ALL
SELECT
    '2024-12-12' AS date, -- Results date
    'RetroPGF Round 6' AS event_type,
    2.4 * 1e6 AS amount,
    'https://github.com/ethereum-optimism/community-hub/blob/main/pages/citizens-house/rounds/retropgf-4.mdx' AS source