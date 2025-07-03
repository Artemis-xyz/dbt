{{
    config(
        materialized="table",
        snowflake_warehouse="ARBITRUM",
        database="arbitrum",
        schema="raw",
        alias="fact_arbitrum_all_supply_events",
    )
}}


WITH date_spine AS (
    SELECT date
    FROM pc_dbt_db.prod.dim_date_spine
    WHERE date BETWEEN '2023-03-21' AND '2030-12-31'
)
, all_events AS (
    SELECT * FROM {{ ref("fact_arbitrum_airdrop_emissions") }}
    UNION ALL
    SELECT * FROM {{ ref("fact_arbitrum_insider_unlocks") }}
    UNION ALL
    SELECT * FROM {{ ref("fact_arbitrum_foundation_unlocks") }}
    UNION ALL
    SELECT * FROM {{ ref("fact_arbitrum_dao_emissions") }}
)
, aggregated_events AS (
    SELECT
        date,
        event_type,
        SUM(amount) AS amount
    FROM all_events
    GROUP BY date, event_type
)
, pivoted_events AS (
    SELECT
        date,
        COALESCE(SUM(CASE WHEN event_type = 'Airdrop' THEN amount ELSE 0 END), 0) AS airdrop_amount,
        COALESCE(SUM(CASE WHEN event_type = 'Team/Investor Unlock' THEN amount ELSE 0 END), 0) AS investor_team_unlock_amount,
        COALESCE(SUM(CASE WHEN event_type = 'Arbitrum Foundation Unlocks' OR event_type = 'Arbitrum Foundation Initial Unlock' THEN amount ELSE 0 END), 0) AS arbitrum_foundation_unlocks_amount,
        COALESCE(SUM(CASE WHEN event_type = 'DAO Emissions' THEN amount ELSE 0 END), 0) AS dao_emissions_amount
    FROM aggregated_events
    GROUP BY date
)
, daily_supply_changes AS (
    SELECT
        ds.date,
        COALESCE(pe.airdrop_amount, 0) AS airdrop_amount,
        COALESCE(pe.investor_team_unlock_amount, 0) AS investor_team_unlock_amount,
        COALESCE(pe.arbitrum_foundation_unlocks_amount, 0) AS arbitrum_foundation_unlocks_amount,
        COALESCE(pe.dao_emissions_amount, 0) AS dao_emissions_amount
    FROM date_spine ds
    LEFT JOIN pivoted_events pe ON ds.date = pe.date
)
, cumulative_supply AS (
    SELECT
        date,
        airdrop_amount,
        investor_team_unlock_amount,
        arbitrum_foundation_unlocks_amount,
        dao_emissions_amount,
        SUM(airdrop_amount) OVER (ORDER BY date) AS cumulative_airdrop_supply,
        SUM(investor_team_unlock_amount) OVER (ORDER BY date) AS cumulative_investor_team_unlock_supply,
        SUM(arbitrum_foundation_unlocks_amount) OVER (ORDER BY date) AS cumulative_arbitrum_foundation_supply,
        SUM(dao_emissions_amount) OVER (ORDER BY date) AS cumulative_dao_emissions_supply,
        SUM(airdrop_amount + investor_team_unlock_amount + arbitrum_foundation_unlocks_amount + dao_emissions_amount) OVER (ORDER BY date) AS total_cumulative_supply, 
        SUM(investor_team_unlock_amount + arbitrum_foundation_unlocks_amount) OVER (ORDER BY date) AS total_vested_supply
    FROM daily_supply_changes
)
SELECT
    date,
    airdrop_amount,
    investor_team_unlock_amount,
    arbitrum_foundation_unlocks_amount,
    dao_emissions_amount,
    cumulative_airdrop_supply,
    cumulative_investor_team_unlock_supply,
    cumulative_arbitrum_foundation_supply,
    cumulative_dao_emissions_supply,
    total_cumulative_supply,
    total_vested_supply
FROM cumulative_supply
ORDER BY date