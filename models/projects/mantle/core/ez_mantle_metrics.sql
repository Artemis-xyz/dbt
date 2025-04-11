-- depends_on {{ ref("ez_mantle_transactions_v2") }}
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
fundamental_data as ({{ get_fundamental_data_for_chain("mantle", "v2") }})
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
, stablecoin_data as ({{ get_stablecoin_metrics("mantle") }})
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
    , fees
    , fees_native
    , l1_data_cost
    , l1_data_cost_native
    , coalesce(fees_native, 0) - l1_data_cost_native as revenue_native -- supply side: fees paid to squencer - fees paied to l1 (L2 Revenue)
    , coalesce(fees, 0) - l1_data_cost as revenue
    , avg_txn_fee
    , treasury_data.treasury_value_native
    , treasury_data.treasury_value_native_change
    , dune_dex_volumes_mantle.dex_volumes
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    , tvl
    -- Chain Usage Metrics
    , dau AS chain_dau
    , wau AS chain_wau
    , mau AS chain_mau
    , txns AS chain_txns
    , returning_users
    , new_users
    , avg_txn_fee AS chain_avg_txn_fee
    , dune_dex_volumes_mantle.dex_volumes AS chain_dex_volumes
    -- Cashflow Metrics
    , fees as chain_fees
    , fees_native AS gross_protocol_revenue_native
    , fees AS gross_protocol_revenue
    , coalesce(fees_native, 0) - l1_data_cost_native as validator_cash_flow_native -- supply side: fees paid to squencer - fees paied to l1 (L2 Revenue)
    , coalesce(fees, 0) - l1_data_cost as validator_cash_flow
    , l1_data_cost_native AS l1_cash_flow_native
    , l1_data_cost AS l1_cash_flow
    -- Protocol Metrics 
    , treasury_data.treasury_value_native AS treasury_native
    , treasury_data.treasury_value_native_change AS treasury_native_change
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    -- Stablecoin Metrics
    , stablecoin_total_supply
    , stablecoin_txns
    , stablecoin_dau
    , stablecoin_mau
    , stablecoin_transfer_volume
    , stablecoin_tokenholder_count
    , artemis_stablecoin_txns
    , artemis_stablecoin_dau
    , artemis_stablecoin_mau
    , artemis_stablecoin_transfer_volume
    , p2p_stablecoin_txns
    , p2p_stablecoin_dau
    , p2p_stablecoin_mau
    , stablecoin_data.p2p_stablecoin_transfer_volume
    , p2p_stablecoin_tokenholder_count
    
from fundamental_data
left join github_data using (date)
left join defillama_data using (date)
left join stablecoin_data using (date)
left join price_data using (date)
left join expenses_data using (date)
left join rolling_metrics using (date)
left join treasury_data using (date)
left join mantle_dex_volumes as dune_dex_volumes_mantle on fundamental_data.date = dune_dex_volumes_mantle.date
where fundamental_data.date < to_date(sysdate())
