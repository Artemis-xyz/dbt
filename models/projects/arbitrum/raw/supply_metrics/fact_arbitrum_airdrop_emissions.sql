{{
    config(
        materialized="table",
        snowflake_warehouse="ARBITRUM",
        database="arbitrum",
        schema="raw",
        alias="fact_arbitrum_airdrop_emissions",
    )
}}

SELECT * FROM (VALUES
    (
        '2023-03-23',
        'Airdrop',
        1.162 * 1e9, -- User airdrop
        'https://docs.arbitrum.foundation/airdrop-eligibility-distribution#distribution-post-aips-11-and-12'
    ),
    (
        '2023-03-23', 
        'Airdrop',
        0.113 * 1e9, -- DAO Airdrop
        'https://docs.arbitrum.foundation/airdrop-eligibility-distribution#distribution-post-aips-11-and-12'
    )
) AS t (date, event_type, amount, source)