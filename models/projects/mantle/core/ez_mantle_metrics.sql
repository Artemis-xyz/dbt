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
, treasury_data as (
    SELECT   
        date,
        sum(native_balance) as treasury_value_native,
        sum(native_balance) - lag(sum(native_balance)) over (order by date) as treasury_value_native_change,
    FROM {{ ref("fact_mantle_treasury_balance") }}
    WHERE token = 'MNT'
    GROUP BY 1
)
, github_data as ({{ get_github_metrics("mantle") }})
, rolling_metrics as ({{ get_rolling_active_address_metrics("mantle") }})
, defillama_data as ({{ get_defillama_metrics("mantle") }})
, price_data as ({{ get_coingecko_metrics("mantle") }})
, mantle_dex_volumes as (
    select date, daily_volume as dex_volumes
    from {{ ref("fact_mantle_daily_dex_volumes") }}
)

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
    , treasury_data.treasury_value_native
    , treasury_data.treasury_value_native_change
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , price
    , market_cap
    , fdmc
    , dune_dex_volumes_mantle.dex_volumes
from fundamental_data
left join github_data using (date)
left join defillama_data using (date)
left join price_data using (date)
left join expenses_data using (date)
left join rolling_metrics using (date)
left join treasury_data using (date)
left join mantle_dex_volumes as dune_dex_volumes_mantle on fundamental_data.date = dune_dex_volumes_mantle.date
where fundamental_data.date < to_date(sysdate())
