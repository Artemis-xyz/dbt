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
    fundamental_data as ({{ get_goldsky_chain_fundamental_metrics("blast", "_v2") }})
    , defillama_data as ({{ get_defillama_metrics("blast") }})
    , market_data as ({{ get_coingecko_metrics("blast") }})
    , eth_price as ({{ get_coingecko_metrics("ethereum") }})
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
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_blast_daily_dex_volumes") }}
    )
    , premine_emissions as (
        select date, premine_unlocks_native, circulating_supply_native
        from {{ ref("fact_blast_daily_supply_data") }}
    )
select
    DATE(coalesce(
        fundamental_data.date,
        defillama_data.date,
        contract_data.date,
        expenses_data.date
    )) as date

    , l1_data_cost_native
    , l1_data_cost
    , coalesce(fees_native, 0) - l1_data_cost_native as revenue_native  -- supply side: fees paid to squencer - fees paied to l1 (L2 Revenue)
    , coalesce(fees, 0) - l1_data_cost as revenue

    -- Standardized Metrics

    -- Market Data 
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume as token_volume
    , tvl

    -- Usage Data
    , daa AS chain_dau
    , daa AS dau
    , txns AS chain_txns
    , txns 
    , dune_dex_volumes_blast.dex_volumes as chain_spot_volume
    , dune_dex_volumes_blast.adjusted_dex_volumes as adjusted_chain_spot_volume

    -- Fee Data
    , fees_native
    , fees_native * eth_price.price AS fees
  
    -- Cashflow metrics
    , fees_native AS ecosystem_revenue_native
    , fees AS ecosystem_revenue
    , revenue_native AS burned_fee_allocation_native
    , revenue AS burned_fee_allocation
    , l1_data_cost_native AS l1_fee_allocation_native
    , l1_data_cost AS l1_fee_allocation
    , revenue_native AS foundation_fee_allocation_native
    , revenue AS foundation_fee_allocation

    -- Supply Metrics
    , premine_unlocks_native
    , premine_unlocks_native as net_supply_change_native
    , circulating_supply_native

    -- Developer metrics
    , weekly_contracts_deployed
    , weekly_contract_deployers

    -- Turnover data
    , market_data.token_turnover_circulating as token_turnover_circulating
    , market_data.token_turnover_fdv as token_turnover_fdv
from fundamental_data
left join defillama_data on fundamental_data.date = defillama_data.date
left join contract_data on fundamental_data.date = contract_data.date
left join expenses_data on fundamental_data.date = expenses_data.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join blast_dex_volumes as dune_dex_volumes_blast on fundamental_data.date = dune_dex_volumes_blast.date
left join market_data on fundamental_data.date = market_data.date
left join eth_price on fundamental_data.date = eth_price.date
left join premine_emissions on fundamental_data.date = premine_emissions.date
where fundamental_data.date < to_date(sysdate())
