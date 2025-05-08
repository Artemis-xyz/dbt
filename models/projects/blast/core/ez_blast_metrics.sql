-- depends_on {{ ref("fact_blast_transactions_v2") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="blast",
        database="blast",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as ({{ get_fundamental_data_for_chain("blast", "v2") }})
    , defillama_data as ({{ get_defillama_metrics("blast") }})
    , price_data as ({{ get_coingecko_metrics("blast") }})
    , contract_data as ({{ get_contract_metrics("blast") }})
    -- NOTE, this says l1 data cost, but that's inaccurate
    -- its both data and execution cost, but I'm following convention for now and we don't publish 
    -- this field anywhere, we only use it to derive revenue
    , expenses_data as (
        select date, chain, l1_data_cost_native, l1_data_cost
        from {{ ref("fact_blast_l1_data_cost") }}
    )  -- supply side revenue and fees
    , rolling_metrics as ({{ get_rolling_active_address_metrics("blast") }})
    , blast_dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_blast_daily_dex_volumes") }}
    )
    , premine_emissions as (
        select date, premine_unlocks_native, circulating_supply_native
        from {{ ref("fact_blast_daily_supply_data") }}
    )
select
    coalesce(
        fundamental_data.date,
        defillama_data.date,
        contract_data.date,
        expenses_data.date
    ) as date
    , 'blast' as chain
    , txns
    , dau
    , wau
    , mau
    , fees_native
    , fees
    , fees / txns as avg_txn_fee
    , median_txn_fee
    , l1_data_cost_native
    , l1_data_cost
    , coalesce(fees_native, 0) - l1_data_cost_native as revenue_native  -- supply side: fees paid to squencer - fees paied to l1 (L2 Revenue)
    , coalesce(fees, 0) - l1_data_cost as revenue
    -- , dau_over_100 omitting balances for blast
    , dune_dex_volumes_blast.dex_volumes as dex_volumes
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    , tvl
    -- Chain Usage metrics
    , txns as chain_txns
    , dau AS chain_dau
    , wau AS chain_wau
    , mau AS chain_mau
    , avg_txn_fee AS chain_avg_txn_fee
    , returning_users
    , new_users
    , low_sleep_users
    , high_sleep_users
    , dau_over_100 AS dau_over_100_balance
    , dune_dex_volumes_blast.dex_volumes as chain_spot_volume
    -- Cashflow metrics
    , fees_native AS gross_protocol_revenue_native
    , fees AS gross_protocol_revenue
    , median_txn_fee AS chain_median_txn_fee
    , revenue_native AS burned_cash_flow_native
    , revenue AS burned_cash_flow
    , l1_data_cost_native AS l1_cash_flow_native
    , l1_data_cost AS l1_cash_flow
    , revenue_native AS foundation_cash_flow_native
    , revenue AS foundation_cash_flow
    -- Developer metrics
    , weekly_contracts_deployed
    , weekly_contract_deployers

    -- Supply Metrics
    , premine_unlocks_native
    , premine_unlocks_native as net_supply_change_native
    , circulating_supply_native
from fundamental_data
left join defillama_data on fundamental_data.date = defillama_data.date
left join contract_data on fundamental_data.date = contract_data.date
left join expenses_data on fundamental_data.date = expenses_data.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join blast_dex_volumes as dune_dex_volumes_blast on fundamental_data.date = dune_dex_volumes_blast.date
left join price_data on fundamental_data.date = price_data.date
left join premine_emissions on fundamental_data.date = premine_emissions.date
where fundamental_data.date < to_date(sysdate())
