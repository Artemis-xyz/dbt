{{
    config(
        materialized='table'
        , snowflake_warehouse='MANTLE'
        , database="mantle"
        , schema="core"
        , alias="ez_metrics"
    )
}}


with fundamental_data as (
    select
        date
        , txns
        , daa as dau
        , gas_usd as fees
        , gas as fees_native
        , revenue
        , l1_data_cost
        , l1_data_cost_native
    from {{ ref("fact_mantle_daa_txns_gas_gas_usd_revenue") }}
)
, github_data as ({{ get_github_metrics("mantle") }})
, defillama_data as ({{ get_defillama_metrics("mantle") }})
, price_data as ({{ get_coingecko_metrics("mantle") }})

select
    fundamental_data.date
    , 'mantle' as chain
    , txns
    , dau
    , fees
    , fees_native
    , revenue
    , l1_data_cost
    , l1_data_cost_native
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
