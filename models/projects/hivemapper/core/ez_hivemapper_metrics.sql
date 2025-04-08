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

, price_data as ({{ get_coingecko_metrics("hivemapper") }})
SELECT
    date_spine.date
    , coalesce(stats.fees, 0) as fees
    , coalesce(stats.supply_side_fees, 0) as primary_supply_side_revenue
    , coalesce(stats.revenue, 0) as revenue
    , coalesce(stats.burn_native, 0) as burns_native
    , coalesce(stats.contributors, 0) as dau

    -- Standardized Metrics

    -- Token Metrics
    , coalesce(price_data.price, 0) as price
    , coalesce(price_data.market_cap, 0) as market_cap
    , coalesce(price_data.fdmc, 0) as fdmc
    , coalesce(price_data.token_volume, 0) as token_volume

    -- Chain Metrics
    , coalesce(stats.contributors, 0) as chain_dau
    
    -- Cash Flow Metrics
    , coalesce(stats.fees, 0) as gross_protocol_revenue
    , coalesce(stats.supply_side_fees, 0) as service_cash_flow
    , coalesce(stats.fees, 0) as burned_cash_flow
    , coalesce(stats.burn_native, 0) as burned_cash_flow_native

    -- Supply Metrics
    , coalesce(stats.mints_native, 0) as mints_native
    , coalesce(stats.mints_native, 0) * coalesce(price_data.price, 0) as mints
    , coalesce(stats.mints_native, 0) - coalesce(stats.burn_native, 0) as net_supply_change_native

    -- Turnover Metrics
    , coalesce(price_data.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(price_data.token_turnover_fdv, 0) as token_turnover_fdv

FROM date_spine
LEFT JOIN stats ON date_spine.date = stats.date
LEFT JOIN price_data ON date_spine.date = price_data.date
WHERE date_spine.date < to_date(sysdate())