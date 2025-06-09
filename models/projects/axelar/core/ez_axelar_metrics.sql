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
    , price_data as ({{ get_coingecko_metrics("axelar") }})
    , supply_data as (
        select * 
        from {{ ref("fact_axelar_supply") }}
    )
select 
    crosschain_data.date
    , 'axelar' as chain
    
    , axelar_chain_data.dau
    , axelar_chain_data.txns
    , crosschain_data.bridge_daa

    , crosschain_data.fees
    , crosschain_data.fees / crosschain_data.bridge_txns as avg_txn_fee
    , validator_fees_data.validator_fees
    

    -- Standardized Metrics
    , crosschain_data.bridge_daa as bridge_dau
    , crosschain_data.bridge_volume as bridge_volume
    , crosschain_data.bridge_txns

    , axelar_chain_data.dau as chain_dau
    , axelar_chain_data.txns as chain_txns

    , crosschain_data.fees as bridge_fees
    , validator_fees_data.validator_fees as chain_fees
    , crosschain_data.fees as ecosystem_revenue
    , crosschain_data.fees / crosschain_data.bridge_txns as chain_avg_txn_fee
    , validator_fees_data.validator_fees as validator_fee_allocation
    , mints_data.mints as gross_emissions_native
    , coalesce(supply_data.totalBurned, 0) as burned_cashflow_native
    , coalesce(supply_data.totalBurned, 0) * price_data.price as burned_cashflow


    , price_data.price as price
    , price_data.market_cap as market_cap
    , price_data.fdmc as fdmc
    , price_data.token_turnover_circulating as token_turnover_circulating
    , price_data.token_turnover_fdv as token_turnover_fdv
    , price_data.token_volume as token_volume

    , coalesce(supply_data.circulatingSupply, 0) - lag(coalesce(supply_data.circulatingSupply, 0)) over (order by crosschain_data.date) as net_supply_change_native
    , coalesce(supply_data.circulatingSupply, 0) as circulating_supply_native

    , github_data.weekly_commits_core_ecosystem
    , github_data.weekly_commits_sub_ecosystem
    , github_data.weekly_developers_core_ecosystem
    , github_data.weekly_developers_sub_ecosystem
from crosschain_data
left join axelar_chain_data using (date)
left join github_data using (date)
left join price_data using (date)
left join validator_fees_data using (date)
left join mints_data using (date)
left join supply_data using (date)
where crosschain_data.date < to_date(sysdate())