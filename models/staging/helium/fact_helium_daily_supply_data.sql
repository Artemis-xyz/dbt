{{ config(materialized="table") }}

with gross_emissions as (
    SELECT
        date,
        sum(emissions_native_to_holders) as emissions_native_to_holders,
        sum(emissions_native_to_treasury) as emissions_native_to_treasury,
        sum(emissions_native_to_holders) + sum(emissions_native_to_treasury) as emissions_native
    FROM {{ ref('fact_helium_gross_emissions') }}
    GROUP BY 1
),
premine_unlocks as (
    SELECT
        date,
        sum(hnt_burned) as burns_native
    FROM {{ ref('fact_helium_token_burns') }}
    GROUP BY 1
)
SELECT
    gross_emissions.date,
    gross_emissions.emissions_native_to_holders,
    gross_emissions.emissions_native_to_treasury,
    gross_emissions.emissions_native,
    CASE
        WHEN gross_emissions.date  = '2023-04-19' THEN 154000000
        ELSE 0
    END as premine_unlocks_native,
    premine_unlocks.burns_native
FROM gross_emissions
LEFT JOIN premine_unlocks ON gross_emissions.date = premine_unlocks.date