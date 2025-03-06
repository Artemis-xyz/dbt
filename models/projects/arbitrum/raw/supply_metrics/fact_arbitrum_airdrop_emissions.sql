{{
    config(
        materialized="table",
        snowflake_warehouse="ARBITRUM",
        database="arbitrum",
        schema="raw",
        alias="fact_arbitrum_airdrop_emissions",
    )
}}

SELECT
    '2023-03-23' AS date,
    'Airdrop' AS event_type,
    1.162 * 1e9 AS amount, -- User airdrop
    'https://docs.arbitrum.foundation/airdrop-eligibility-distribution#distribution-post-aips-11-and-12' AS source
UNION ALL
SELECT
    '2023-03-23' AS date,
    'Airdrop' AS event_type,
    0.113 * 1e9 AS amount, -- DAO Airdrop
    'https://docs.arbitrum.foundation/airdrop-eligibility-distribution#distribution-post-aips-11-and-12' AS source