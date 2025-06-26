{{
    config(
        materialized = "table",
        snowflake_warehouse = "METIS",
        database = "METIS",
        schema = "core",
        alias = "ez_metrics"
    )
}}

with fees as (
    select
        date,
        fees_usd
    from {{ref("fact_metis_fees")}}
),
txns as (
    select
        date,
        txns
    from {{ref("fact_metis_txns")}}
)
, daus as (
    select
        date,
        dau
    from {{ref("fact_metis_dau")}}
)
, defillama_data as (
    {{ get_defillama_metrics("metis") }}
)
, supply_data as (
    select
        date
        , premine_unlocks_native
        , net_supply_change_native
        , circulating_supply_native
    from {{ref("fact_metis_supply_data")}}
)
, price_data as ({{ get_coingecko_metrics("metis-token") }})

select
    coalesce(fees.date, txns.date, daus.date) as date
    , dau
    , txns
    , fees_usd as fees
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    -- Chain Usage Metrics
    , dau as chain_dau
    , txns as chain_txns
    , defillama_data.dex_volumes as chain_dex_volumes
    , defillama_data.tvl as tvl
    -- Cashflow Metrics
    , fees_usd as chain_fees
    , fees_usd as ecosystem_revenue
    , fees_usd * 0.7 as validator_fee_allocation
    , fees_usd * 0.3 as other_fee_allocation
 
    -- Supply Metrics
    , premine_unlocks_native
    , net_supply_change_native
    , circulating_supply_native
 
    -- Other Metrics
    , token_turnover_circulating
    , token_turnover_fdv
from fees
left join txns on fees.date = txns.date
left join daus on fees.date = daus.date 
left join price_data on fees.date = price_data.date
left join defillama_data on fees.date = defillama_data.date
left join supply_data on fees.date = supply_data.date