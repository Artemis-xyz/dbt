{{
    config(
        materialized="table"
        , snowflake_warehouse="AXELAR"
        , database="axelar"
        , schema="core"
        , alias="ez_metrics"
    )
}}

with
    crosschain_data as (
        select
            date
            , bridge_txns
            , bridge_daa
            , volume as bridge_volume
            , fees
        from {{ ref("fact_axelar_crosschain_dau_txns_fees_volume") }}
    )
    , axelar_chain_data as (
        select
            date
            , txns
            , daa as dau
        from {{ ref("fact_axelar_daa_txns") }}
    )
    , mints_data as (
        select
            date
            , mints
        from {{ ref("fact_axelar_mints") }}
    )
    , validator_fees_data as (
        select
            date
            , validator_fees
        from {{ ref("fact_axelar_validator_fees") }}
    )
    , github_data as ({{ get_github_metrics("Axelar Network") }})
    , market_data as ({{ get_coingecko_metrics("axelar") }})
    , supply_data as (
        select * 
        from {{ ref("fact_axelar_supply") }}
    )
select 
    crosschain_data.date

    -- Standardized Metrics

    -- Market Data
    , market_data.price as price
    , market_data.market_cap as market_cap
    , market_data.fdmc as fdmc
    , market_data.token_volume as token_volume

    -- Usage Data 
    , crosschain_data.bridge_daa as bridge_dau
    , axelar_chain_data.dau as chain_dau
    , coalesce(crosschain_data.bridge_daa, 0) + coalesce(axelar_chain_data.dau, 0) as dau
    , crosschain_data.bridge_txns
    , axelar_chain_data.txns as chain_txns
    , coalesce(crosschain_data.bridge_txns, 0) + coalesce(axelar_chain_data.txns, 0) as txns
    , crosschain_data.bridge_volume as bridge_volume
    , crosschain_data.bridge_volume as volume

    -- Fee Data
    , crosschain_data.fees as bridge_fees
    , validator_fees_data.validator_fees as chain_fees
    , coalesce(crosschain_data.fees, 0) + coalesce(validator_fees_data.validator_fees, 0) as fees
    , coalesce(supply_data.totalBurned, 0) * market_data.price as burned_fee_allocation

    -- Financial Statements
    , coalesce(supply_data.totalBurned, 0) * market_data.price as revenue

    -- Supply Data
    , mints_data.mints as gross_emissions_native
    , coalesce(supply_data.circulatingSupply, 0) - lag(coalesce(supply_data.circulatingSupply, 0)) over (order by crosschain_data.date) as net_supply_change_native
    , coalesce(supply_data.circulatingSupply, 0) as circulating_supply_native

    -- Developer Data
    , github_data.weekly_commits_core_ecosystem
    , github_data.weekly_commits_sub_ecosystem
    , github_data.weekly_developers_core_ecosystem
    , github_data.weekly_developers_sub_ecosystem

    -- Token Turnover Data
    , market_data.token_turnover_circulating as token_turnover_circulating
    , market_data.token_turnover_fdv as token_turnover_fdv

from crosschain_data
left join axelar_chain_data using (date)
left join github_data using (date)
left join market_data using (date)
left join validator_fees_data using (date)
left join mints_data using (date)
left join supply_data using (date)
where crosschain_data.date < to_date(sysdate())