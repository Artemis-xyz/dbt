{{
    config(
        materialized='table'
        , snowflake_warehouse='MANTLE'
        , database="mantle"
        , schema="core"
        , alias="ez_metrics"
    )
}}


with 
fundamental_data as ({{ get_fundamental_data_for_chain("mantle") }})
, expenses_data as (
    select date, chain, l1_data_cost_native, l1_data_cost
    from {{ ref("fact_mantle_l1_data_cost") }}
)
, github_data as ({{ get_github_metrics("mantle") }})
, rolling_metrics as ({{ get_rolling_active_address_metrics("mantle") }})
, defillama_data as ({{ get_defillama_metrics("mantle") }})
, price_data as ({{ get_coingecko_metrics("mantle") }})

select
    fundamental_data.date
    , 'mantle' as chain
    , txns
    , dau
    , wau
    , mau
    , new_users
    , returning_users
    , fees
    , fees_native
    , l1_data_cost
    , l1_data_cost_native
    , coalesce(fees_native, 0) - l1_data_cost_native as revenue_native -- supply side: fees paid to squencer - fees paied to l1 (L2 Revenue)
    , coalesce(fees, 0) - l1_data_cost as revenue
    , avg_txn_fee
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
left join expenses_data using (date)
left join rolling_metrics using (date)
where fundamental_data.date < to_date(sysdate())
