{{
    config(
        materialized="table"
        , snowflake_warehouse="MULTIVERSX"
        , database="multiversx"
        , schema="core"
        , alias="ez_metrics"
    )
}}

with
    fundamental_data as (
        select
            date
            , txns
            , daa as dau
        from {{ ref("fact_multiversx_txns") }}
        left join {{ ref("fact_multiversx_daa") }} using (date)
    )
    , github_data as ({{ get_github_metrics("elrond") }})
    , defillama_data as ({{ get_defillama_metrics("elrond") }})
    , price_data as ({{ get_coingecko_metrics("elrond-erd-2") }})

select
    fundamental_data.date
    , 'multiversx' as chain
    , txns
    , dau
    , tvl
    , dex_volumes
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , price
    , market_cap
    , fdmc
from fundamental_data
left join github_data using (date)
left join defillama_data using (date)
left join price_data using (date)
where fundamental_data.date < to_date(sysdate())
