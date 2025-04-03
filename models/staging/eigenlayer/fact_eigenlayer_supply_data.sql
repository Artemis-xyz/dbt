{{
    config(
        materialized="table",
        alias="fact_eigenlayer_supply_data",
    )
}}

SELECT
    date,
    -- Parse the numeric values, removing commas and converting to proper numbers
    REPLACE(emissions_native, ',', '')::FLOAT AS emissions_native,
    REPLACE(premine_unlocks_native, ',', '')::FLOAT AS premine_unlocks_native,
    REPLACE(net_supply_change_native, ',', '')::FLOAT AS net_supply_change_native,
    REPLACE(circulating_supply, ',', '')::FLOAT AS circulating_supply
FROM {{ source('MANUAL_STATIC_TABLES', 'eigenlayer_daily_supply_data') }}
WHERE date <= CURRENT_DATE() -- Only include data up to the current date
ORDER BY date