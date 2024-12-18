{{
    config(
        materialized="table",
        snowflake_warehouse="HIVEMAPPER",
        database="hivemapper",
        schema="core",
        alias="ez_metrics",
    )
}}

with 
    date_spine as ( 
        select * from {{ ref('dim_date_spine') }}
        where date between '2022-12-20' and to_date(sysdate())
    )

, bme_reward_types_cte as (
    SELECT
        distinct reward_type as bme_reward_types
    FROM 
        {{ ref('fact_hivemapper_stats') }}
    WHERE reward_type like 'Honey Burst%' or reward_type like 'Map Consumption%'
)
, stats as (
    select
        block_timestamp::date as date,
        SUM(
            CASE
                WHEN reward_type in (SELECT bme_reward_types FROM bme_reward_types_cte) AND action = 'mint' THEN amount_usd
            END
        ) as supply_side_fees,
        SUM(
            CASE
                WHEN action = 'burn' and block_timestamp::date >= '2024-04-17' THEN amount_usd * 0.75 -- MIP 15 was introduced on 2024-04-17 and changed the burn fee structure such that 75% of burns are permanent (prev 0%)
            END
        ) as revenue,
        SUM(
            CASE
                WHEN action = 'burn' THEN amount_usd -- MIP 15 was introduced on 2024-04-17 and changed the burn fee structure such that 75% of burns are permanent (prev 0%)
            END
        ) as fees,
        SUM(
            CASE WHEN action = 'mint'
                THEN amount_native
            END
        ) as mints_native,
        SUM(
            CASE WHEN action = 'burn'
                THEN amount_native
            END
        ) as burn_native,
        COUNT( distinct
            CASE WHEN action = 'mint'
                THEN tx_to_account
            WHEN action = 'transfer' AND reward_type in ('Bounty')
                THEN tx_to_account
            END
        ) AS contributors
    from
        {{ ref('fact_hivemapper_stats') }}
    GROUP BY
        1
)
SELECT
    date_spine.date,
    COALESCE(stats.fees, 0) as fees,
    COALESCE(stats.supply_side_fees, 0) as supply_side_fees,
    COALESCE(stats.revenue, 0) as revenue,
    COALESCE(stats.mints_native, 0) as mints_native,
    COALESCE(stats.burn_native, 0) as burn_native,
    COALESCE(stats.contributors, 0) as dau
FROM date_spine
LEFT JOIN stats ON date_spine.date = stats.date
WHERE date_spine.date < to_date(sysdate())