{{
    config(
        materialized="table",
        snowflake_warehouse="MANTA",
        database="manta",
        schema="core",
        alias="ez_metrics"
    )
}}
with 
    price_data as ({{ get_coingecko_metrics("manta-network") }})
   , defillama_data as ({{ get_defillama_metrics("manta") }})
   , supply_data as (
        select
            date
            , gross_emissions_native
            , premine_unlocks_native
            , net_supply_change_native
            , circulating_supply_native
        from {{ ref('fact_manta_supply_data') }}
    )
SELECT
    date
    , daily_txns as txns
    , dau
    , fees
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    -- Chain Usage Metrics
    , txns AS chain_txns
    , dau AS chain_dau
    , dex_volumes as chain_spot_volume
    , tvl
    -- Cashflow Metrics
    , fees as chain_fees
    , fees AS ecosystem_revenue
    , case when date > '2024-09-30' then fees / 2 else fees end as equity_fee_allocation
    , case when date > '2024-09-30' then fees / 2 else 0 end as user_fee_allocation

    -- Supply Data
    , gross_emissions_native
    , premine_unlocks_native
    , net_supply_change_native
    , circulating_supply_native
FROM {{ ref('fact_manta_txns_daa') }}
LEFT JOIN price_data using (date)
LEFT JOIN defillama_data using (date)
LEFT JOIN supply_data using (date)
WHERE date < to_date(sysdate())
