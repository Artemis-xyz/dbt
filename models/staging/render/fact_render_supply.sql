{{
    config(
        materialized="table",
        snowflake_warehouse="RENDER",
    )
}}

with total_supply_by_chain as (
    SELECT date, chain, supply_native FROM {{ ref("fact_render_solana_supply") }}
    UNION ALL
    SELECT date, chain, supply_native FROM {{ ref("fact_render_ethereum_supply") }}
    UNION ALL
    SELECT date, chain, supply_native FROM {{ ref("fact_render_polygon_supply") }}
)
, total_supply as (
    SELECT
        date,
        sum(supply_native) as total_supply_native
    FROM total_supply_by_chain
    GROUP BY 1
)
, treasury_by_chain as (
    SELECT
        date,
        sum(balance_native) as treasury_native
    FROM {{ ref("fact_render_treasury_balance_ethereum") }}
    WHERE lower(contract_address) in(
        lower('0x6De037ef9aD2725EB40118Bb1702EBb27e4Aeb24'),
        lower('0x0996bFb5D057faa237640E2506BE7B4f9C46de0B')
    )
    GROUP BY 1
    UNION ALL
    SELECT
        date,
        sum(balance_raw) as treasury_native
    FROM {{ ref("fact_render_treasury_balance_solana") }}
    WHERE lower(contract_address) = lower('rndrizKT3MK1iimdxRdWabcF7Zg7AR5T4nud4EkHBof')
    GROUP BY 1
    UNION ALL

    -- Hardcoded values because during Ethereum -> Solana migration funds get commingled in a different address for 3 days
    SELECT date, treasury_native
    FROM VALUES 
        ('2024-09-12', 100000000),
        ('2024-09-13', 100000000),
        ('2024-09-14', 100000000)
    as t(date, treasury_native))
, treasury as (
    SELECT
        date,
        sum(treasury_native) as treasury_native
    FROM treasury_by_chain
    GROUP BY 1
)
SELECT
    date,
    total_supply_native,
    treasury_native,
    total_supply_native - coalesce(treasury_native, 0) as issued_supply_native,
    total_supply_native - coalesce(treasury_native, 0) as circulating_supply_native
FROM total_supply
LEFT JOIN treasury USING(date)