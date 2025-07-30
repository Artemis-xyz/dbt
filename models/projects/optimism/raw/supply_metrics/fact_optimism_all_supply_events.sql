{{
    config(
        materialized="table",
        snowflake_warehouse="OPTIMISM",
        database="optimism",
        schema="raw",
        alias="fact_optimism_all_supply_events",
    )
}}

WITH date_spine AS (
    SELECT date
    FROM pc_dbt_db.prod.dim_date_spine
    WHERE date BETWEEN '2022-05-31' AND '2030-12-31'
)
, all_events AS (

    SELECT 
        date,
        CASE 
            WHEN event_type LIKE 'RetroPGF%' THEN 'RetroPGF'
            WHEN event_type LIKE 'GovGrants%' THEN 'GovGrants'
            ELSE event_type 
        END AS event_type_grouped,
        amount
    FROM (
        SELECT * FROM {{ ref("fact_optimism_govgrants_emissions") }}
        UNION ALL
        SELECT * FROM {{ ref("fact_optimism_base_grants_emissions") }}
        UNION ALL
        SELECT * FROM {{ ref("fact_optimism_private_sale_emissions") }}
        UNION ALL
        SELECT * FROM {{ ref("fact_optimism_insider_unlocks") }}
        UNION ALL
        SELECT * FROM {{ ref("fact_optimism_airdrop_emissions") }}
        UNION ALL
        SELECT * FROM {{ ref("fact_optimism_retropgf_emissions") }}
    ) AS aggregated_events
)
, aggregated_events AS (
    SELECT 
        ds.date,
        COALESCE(SUM(CASE WHEN event_type_grouped = 'Airdrop' THEN amount END), 0) AS airdrop_supply,
        COALESCE(SUM(CASE WHEN event_type_grouped = 'RetroPGF' THEN amount END), 0) AS retropgf_supply,
        COALESCE(SUM(CASE WHEN event_type_grouped = 'GovGrants' THEN amount END), 0) AS gov_grants_supply,
        COALESCE(SUM(CASE WHEN event_type_grouped = 'Early Core Contributors Unlocks' THEN amount END), 0) AS core_contributor_unlocks_supply,
        COALESCE(SUM(CASE WHEN event_type_grouped = 'Investor Unlocks' THEN amount END), 0) AS investor_unlocks_supply,
        COALESCE(SUM(CASE WHEN event_type_grouped = 'Base Grant' THEN amount END), 0) AS base_grant_supply,
        COALESCE(SUM(CASE WHEN event_type_grouped = 'Private Token Sale' THEN amount END), 0) AS private_sale_supply
    FROM date_spine ds
    LEFT JOIN all_events ae ON ds.date = ae.date
    GROUP BY ds.date
),

-- Compute cumulative sum per event type & total supply
cumulative_supply AS (
    SELECT 
        date,
        airdrop_supply,
        retropgf_supply,
        gov_grants_supply,
        core_contributor_unlocks_supply,
        investor_unlocks_supply,
        base_grant_supply,
        private_sale_supply,

        -- Cumulative sum per category
        SUM(airdrop_supply) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS airdrop_cum_sum,
        SUM(retropgf_supply) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS retropgf_cum_sum,
        SUM(gov_grants_supply) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS gov_grants_cum_sum,
        SUM(core_contributor_unlocks_supply) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS core_contributor_unlocks_cum_sum,
        SUM(investor_unlocks_supply) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS investor_unlocks_cum_sum,
        SUM(base_grant_supply) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS base_grant_cum_sum,
        SUM(private_sale_supply) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS private_sale_cum_sum,

        -- Ensure total supply correctly sums all event types
        (airdrop_supply + retropgf_supply + gov_grants_supply + core_contributor_unlocks_supply + investor_unlocks_supply + base_grant_supply + private_sale_supply) AS total_daily_supply,

        -- Correct total circulating supply
        SUM(
            COALESCE(airdrop_supply, 0) + 
            COALESCE(retropgf_supply, 0) + 
            COALESCE(gov_grants_supply, 0) + 
            COALESCE(core_contributor_unlocks_supply, 0) + 
            COALESCE(investor_unlocks_supply, 0) + 
            COALESCE(base_grant_supply, 0) + 
            COALESCE(private_sale_supply, 0)
        ) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_circulating_supply, 

        SUM( 
            COALESCE(core_contributor_unlocks_supply, 0) + 
            COALESCE(investor_unlocks_supply, 0)
        ) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_vested_supply
    FROM aggregated_events
)

SELECT * FROM cumulative_supply
ORDER BY date