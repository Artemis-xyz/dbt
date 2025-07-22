-- depends_on: {{ ref("fact_polkadot_rolling_active_addresses") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="POLKADOT",
        database="polkadot",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as (
        select
            date,
            chain,
            txns,
            dau,
            fees_native,
            fees_usd as fees,
            burns,
            fees_usd * .8 as revenue,
            fees_native * .8 as revenue_native
        from {{ ref("fact_polkadot_fundamental_metrics") }}
    ),
    collectives_fundamental_data as (
        select
            date, 
            txns,
            dau, 
            fees_native, 
            fees_usd as fees
        from {{ ref("fact_collectives_fundamental_metrics") }}
    ),
    people_fundamental_data as (
        select
            date, 
            txns,
            dau, 
            fees_native, 
            fees_usd as fees
        from {{ ref("fact_people_fundamental_metrics") }}
    ),
    coretime_fundamental_data as (
        select
            date, 
            txns,
            dau, 
            fees_native, 
            fees_usd as fees
        from {{ ref("fact_coretime_fundamental_metrics") }}
    ),
    bridgehub_fundamental_data as (
        select
            date, 
            txns,
            dau, 
            fees_native, 
            fees_usd as fees
        from {{ ref("fact_polkadot_bridgehub_fundamental_metrics") }}
    ),
    asset_hub_fundamental_data as (
        select
            date,
            txns,
            dau, 
            fees_native, 
            fees_usd as fees
        from {{ ref("fact_polkadot_asset_hub_fundamental_metrics") }}
    ),
    issued_supply_metrics as (
        select 
            date,
            max_supply_to_date as max_supply_native,
            total_supply_to_date as total_supply_native,
            issued_supply as issued_supply_native,
            float as circulating_supply_native
        from {{ ref("fact_polkadot_issued_supply_and_float") }}
    ),
    price_data as ({{ get_coingecko_metrics("polkadot") }}),
    defillama_data as ({{ get_defillama_metrics("polkadot") }}),
    github_data as ({{ get_github_metrics("polkadot") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("polkadot") }})
select
    fundamental_data.date
    , fundamental_data.chain
    , coalesce(fundamental_data.txns,0) + coalesce(collectives_fundamental_data.txns,0) + coalesce(people_fundamental_data.txns, 0) + coalesce(coretime_fundamental_data.txns, 0) + coalesce(bridgehub_fundamental_data.txns, 0) + coalesce(asset_hub_fundamental_data.txns, 0) as txns
    , coalesce(fundamental_data.dau,0) + coalesce(collectives_fundamental_data.dau,0) + coalesce(people_fundamental_data.dau,0) + coalesce(coretime_fundamental_data.dau,0) + coalesce(bridgehub_fundamental_data.dau,0) + coalesce(asset_hub_fundamental_data.dau,0) as dau
    , wau
    , mau
    , coalesce(fundamental_data.fees_native,0) + coalesce(collectives_fundamental_data.fees_native,0) + coalesce(people_fundamental_data.fees_native,0) + coalesce(coretime_fundamental_data.fees_native,0) + coalesce(bridgehub_fundamental_data.fees_native,0) + coalesce(asset_hub_fundamental_data.fees_native,0) as fees_native
    , coalesce(fundamental_data.fees,0) + coalesce(collectives_fundamental_data.fees,0) + coalesce(people_fundamental_data.fees,0) + coalesce(coretime_fundamental_data.fees,0) + coalesce(bridgehub_fundamental_data.fees,0) + coalesce(asset_hub_fundamental_data.fees,0) as fees
    , revenue_native
    , revenue
    , (coalesce(fundamental_data.fees,0) + coalesce(collectives_fundamental_data.fees,0) + coalesce(people_fundamental_data.fees,0) + coalesce(coretime_fundamental_data.fees,0) + coalesce(bridgehub_fundamental_data.fees,0) + coalesce(asset_hub_fundamental_data.fees,0)) / (coalesce(fundamental_data.txns,0) + coalesce(collectives_fundamental_data.txns,0) + coalesce(people_fundamental_data.txns, 0) + coalesce(coretime_fundamental_data.txns, 0) + coalesce(bridgehub_fundamental_data.txns, 0) + coalesce(asset_hub_fundamental_data.txns, 0)) as avg_txn_fee
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    , tvl
    -- Chain Usage Metrics
    ,coalesce(fundamental_data.dau,0) + coalesce(collectives_fundamental_data.dau,0) + coalesce(people_fundamental_data.dau,0) + coalesce(coretime_fundamental_data.dau,0) + coalesce(bridgehub_fundamental_data.dau,0) + coalesce(asset_hub_fundamental_data.dau,0) as chain_dau
    , wau AS chain_wau
    , mau AS chain_mau
    , coalesce(fundamental_data.txns,0) + coalesce(collectives_fundamental_data.txns,0) + coalesce(people_fundamental_data.txns, 0) + coalesce(coretime_fundamental_data.txns, 0) + coalesce(bridgehub_fundamental_data.txns, 0) + coalesce(asset_hub_fundamental_data.txns, 0) as chain_txns
    , (coalesce(fundamental_data.fees,0) + coalesce(collectives_fundamental_data.fees,0) + coalesce(people_fundamental_data.fees,0) + coalesce(coretime_fundamental_data.fees,0) + coalesce(bridgehub_fundamental_data.fees,0) + coalesce(asset_hub_fundamental_data.fees,0)) / (coalesce(fundamental_data.txns,0) + coalesce(collectives_fundamental_data.txns,0) + coalesce(people_fundamental_data.txns, 0) + coalesce(coretime_fundamental_data.txns, 0) + coalesce(bridgehub_fundamental_data.txns, 0) + coalesce(asset_hub_fundamental_data.txns, 0)) as chain_avg_txn_fee 
    -- Cash Flow Metrics
    , coalesce(fundamental_data.fees,0) + coalesce(collectives_fundamental_data.fees,0) + coalesce(people_fundamental_data.fees,0) + coalesce(coretime_fundamental_data.fees,0) + coalesce(bridgehub_fundamental_data.fees,0) + coalesce(asset_hub_fundamental_data.fees,0) as chain_fees
    , coalesce(fundamental_data.fees_native,0) + coalesce(collectives_fundamental_data.fees_native,0) + coalesce(people_fundamental_data.fees_native,0) + coalesce(coretime_fundamental_data.fees_native,0) + coalesce(bridgehub_fundamental_data.fees_native,0) + coalesce(asset_hub_fundamental_data.fees_native,0) as ecosystem_revenue_native
    , chain_fees as ecosystem_revenue
    , revenue_native AS treasury_fee_allocation_native
    , revenue AS treasury_fee_allocation

    -- Issued Supply Metrics
    , issued_supply_metrics.max_supply_native
    , issued_supply_metrics.total_supply_native
    , issued_supply_metrics.issued_supply_native
    , issued_supply_metrics.circulating_supply_native

    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
from fundamental_data
left join collectives_fundamental_data on fundamental_data.date = collectives_fundamental_data.date
left join people_fundamental_data on fundamental_data.date = people_fundamental_data.date
left join coretime_fundamental_data on fundamental_data.date = coretime_fundamental_data.date
left join bridgehub_fundamental_data on fundamental_data.date = bridgehub_fundamental_data.date
left join asset_hub_fundamental_data on fundamental_data.date = asset_hub_fundamental_data.date
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join github_data on fundamental_data.date = github_data.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join issued_supply_metrics on fundamental_data.date = issued_supply_metrics.date
where fundamental_data.date < to_date(sysdate())
