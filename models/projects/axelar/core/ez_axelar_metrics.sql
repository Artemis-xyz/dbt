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
select 
    crosschain_data.date
    , 'axelar' as chain
    , crosschain_data.bridge_txns
    , axelar_chain_data.dau
    , axelar_chain_data.txns
    , crosschain_data.bridge_daa
    , crosschain_data.bridge_volume
    , crosschain_data.fees
    , crosschain_data.fees / crosschain_data.bridge_txns as avg_txn_fee
    , validator_fees_data.validator_fees
    , mints_data.mints
    , github_data.weekly_commits_core_ecosystem
    , github_data.weekly_commits_sub_ecosystem
    , github_data.weekly_developers_core_ecosystem
    , github_data.weekly_developers_sub_ecosystem
    , price_data.price
    , price_data.market_cap
from crosschain_data
left join axelar_chain_data using (date)
left join github_data using (date)
left join price_data using (date)
left join validator_fees_data using (date)
left join mints_data using (date)
where crosschain_data.date < to_date(sysdate())