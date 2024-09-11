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
    fundamental_data as (
        select
            date
            , txns
            , dau
            , fees
        from {{ ref("fact_axelar_crosschain_dau_txns_fees_volume") }}
    )
    , github_data as ({{ get_github_metrics("Axelar Network") }})
    , price_data as ({{ get_coingecko_metrics("axelar") }})
select 
    fundamental_data.date
    , 'axelar' as chain
    , txns
    , dau
    , fees
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , price
    , market_cap
    , fdmc
from fundamental_data
left join github_data using (date)
left join price_data using (date)
where fundamental_data.date < to_date(sysdate())