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
    , github_data as ({{ get_github_metrics("Axelar Network") }})
    , price_data as ({{ get_coingecko_metrics("axelar") }})
select 
    crosschain_data.date
    , 'axelar' as chain
    , crosschain_data.bridge_txns
    , axelar_chain_data.dau
    , axelar_chain_data.txns
    , crosschain_data.bridge_daa
    , crosschain_data.fees
    , crosschain_data.fees / crosschain_data.bridge_txns as avg_txn_fee
    , github_data.weekly_commits_core_ecosystem
    , github_data.weekly_commits_sub_ecosystem
    , github_data.weekly_developers_core_ecosystem
    , github_data.weekly_developers_sub_ecosystem
    , price_data.price
    , price_data.market_cap
    , fdmc
from crosschain_data
left join axelar_chain_data using (date)
left join github_data using (date)
left join price_data using (date)
where crosschain_data.date < to_date(sysdate())