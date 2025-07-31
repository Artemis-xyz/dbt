{{
    config(
        materialized="incremental",
        snowflake_warehouse="STARGATE",
        database="stargate",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

WITH 
-- First seen date for each address
first_seen AS (
    SELECT 
        src_address, 
        min(src_block_timestamp::date) AS first_seen_date
    FROM {{ ref("fact_stargate_v2_transfers") }}
    GROUP BY src_address
)

-- New addresses per day (first transaction)
, new_addresses AS (
    SELECT 
        first_seen_date AS transaction_date, 
        COUNT(DISTINCT src_address) AS new_addresses
    FROM first_seen
    GROUP BY transaction_date
)

-- Returning addresses per day (transactions AFTER first_seen_date)
, returning_addresses AS (
    SELECT 
        src_block_timestamp::date AS transaction_date,
        COUNT(DISTINCT t.src_address) AS returning_addresses
    FROM {{ ref("fact_stargate_v2_transfers") }} t
    JOIN first_seen f 
        ON t.src_address = f.src_address
        AND t.src_block_timestamp::date > f.first_seen_date
    GROUP BY transaction_date
)

-- Daily metrics with modified treasury_fee calculation
, daily_metrics AS (
    SELECT 
        t.dst_block_timestamp::date AS transaction_date,
        COUNT(*) AS daily_transactions,
        AVG(amount_sent) AS avg_daily_transaction_size,
        SUM(amount_sent) AS daily_volume,
        COUNT(DISTINCT src_address) AS daily_active_addresses,
        SUM(daily_active_addresses) OVER (ORDER BY transaction_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
        AS cumulative_active_addresses,
        SUM(token_rewards) AS token_rewards,
        -- v2 fees (fee allocation breakdown) -veSTG Holders (1/6) of all fees generated & Protocol Treasury (5/6) of all fees generated
        SUM(fees) AS fees,
        SUM(fees) * 1/6 AS supply_side_fee,
        SUM(fees) * 5/6 AS revenue,
    FROM {{ ref("fact_stargate_v2_transfers") }} t
    GROUP BY transaction_date
)
, treasury_models as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("fact_stargate_v2_arbitrum_treasury_balance"),
                ref("fact_stargate_v2_avalanche_treasury_balance"),
                ref("fact_stargate_v2_base_treasury_balance"),
                ref("fact_stargate_v2_bsc_treasury_balance"),
                ref("fact_stargate_v2_ethereum_treasury_balance"),
                ref("fact_stargate_v2_optimism_treasury_balance"),
                ref("fact_stargate_v2_polygon_treasury_balance"),
                ref("fact_stargate_v2_mantle_treasury_balance"),
            ],
        )
    }}
)
, treasury_metrics as (
    select
        date
        , sum(balance) as treasury_usd
    from treasury_models
    where balance > 2 and balance is not null
    group by date
)

, tvl_models as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("fact_stargate_v2_arbitrum_tvl"),
                ref("fact_stargate_v2_avalanche_tvl"),
                ref("fact_stargate_v2_base_tvl"),
                ref("fact_stargate_v2_bsc_tvl"),
                ref("fact_stargate_v2_ethereum_tvl"),
                ref("fact_stargate_v2_optimism_tvl"),
                ref("fact_stargate_v2_polygon_tvl"),
                ref("fact_stargate_v2_mantle_tvl"),
                ref("fact_stargate_v2_sei_tvl"),
            ],
        )
    }}
)
, total_stg_staked as (
    select *
    from {{ ref("fact_stargate_polygon_stg_balances") }}
    where lower(address) = lower('0x3ab2da31bbd886a7edf68a6b60d3cde657d3a15d')
    union all
    select *
    from {{ ref("fact_stargate_arbitrum_stg_balances") }}
    where lower(address) = lower('0xfbd849e6007f9bc3cc2d6eb159c045b8dc660268')
    union all
    select *
    from {{ ref("fact_stargate_avalanche_stg_balances") }}
    where lower(address) = lower('0xca0f57d295bbce554da2c07b005b7d6565a58fce')
    union all
    select *
    from {{ ref("fact_stargate_bsc_stg_balances") }}
    where lower(address) = lower('0xD4888870C8686c748232719051b677791dBDa26D')
    union all
    select *
    from {{ ref("fact_stargate_ethereum_stg_balances") }}
    where lower(address) = lower('0x0e42acbd23faee03249daff896b78d7e79fbd58e')
    union all
    select *
    from {{ ref("fact_stargate_optimism_stg_balances") }}
    where lower(address) = lower('0x43d2761ed16c89a2c4342e2b16a3c61ccf88f05b')
)
, total_stg_staked_metrics as (
    select
        date
        , sum(balance_native) as staked_native
        , sum(balance) as staked_usd
    from total_stg_staked
    group by date
)
, tvl_metrics as (
    select
        date
        , sum(balance) as tvl
    from tvl_models
    where balance > 2 and balance is not null
    group by date
)
-- Weekly metrics (directly from raw data)
, weekly_metrics AS (
    SELECT 
        DATE_TRUNC('week', src_block_timestamp) AS week_start,
        COUNT(DISTINCT src_address) AS weekly_active_addresses
    FROM {{ ref("fact_stargate_v2_transfers") }}
    GROUP BY week_start
)

-- Monthly metrics (directly from raw data)
, monthly_metrics AS (
    SELECT 
        DATE_TRUNC('month', src_block_timestamp) AS month_start,
        COUNT(DISTINCT src_address) AS monthly_active_addresses
    FROM {{ ref("fact_stargate_v2_transfers") }}
    GROUP BY month_start
)

-- Daily growth percentages
, daily_growth AS (
    SELECT 
        transaction_date, 
        daily_transactions,
        avg_daily_transaction_size,
        daily_volume,
        daily_active_addresses,
        cumulative_active_addresses,
        revenue,
        supply_side_fee,
        fees,
        token_rewards
    FROM daily_metrics
)

-- Transaction Bucket Counts (Pre-Aggregated)
, transaction_bucket_counts AS (
    SELECT 
        src_block_timestamp::date AS transaction_date,
        COUNT(CASE WHEN amount_sent < 100 THEN 1 END) AS count_0_100,
        COUNT(CASE WHEN amount_sent BETWEEN 100 AND 1000 THEN 1 END) AS count_100_1K,
        COUNT(CASE WHEN amount_sent BETWEEN 1000 AND 10000 THEN 1 END) AS count_1K_10K,
        COUNT(CASE WHEN amount_sent BETWEEN 10000 AND 100000 THEN 1 END) AS count_10K_100K,
        COUNT(CASE WHEN amount_sent >= 100000 THEN 1 END) AS count_100K_plus
    FROM {{ ref("fact_stargate_v2_transfers") }}
    GROUP BY transaction_date
)
, circulating_supply_metrics as (
    select
        date
        , sum(circulating_supply) as circulating_supply
    from {{ ref("fact_stargate_circulating_supply") }}
    group by date
)
, supply_data as (
    select 
        date
        , gross_emissions_native
        , premine_unlocks_native
        , burns_native
        , net_supply_change_native
        , circulating_supply_native
    from {{ ref("fact_stargate_supply_data") }}
)
, market_metrics as ({{ get_coingecko_metrics("stargate-finance") }})
, hydra_models as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("fact_stargate_v2_berachain_hydra_assets"),
                ref("fact_stargate_v2_sei_hydra_assets"),
            ],
        )
    }}
)
, hydra_metrics as (
    select
        date
        , sum(amount) as hydra_locked_assets
    from hydra_models
    group by date
)
-- Final output with simplified GROUP BY
SELECT 
    t.date as date,
    'stargate' as artemis_id
    
    --Standardized Metrics

    -- Market Data
    , market_metrics.token_volume
    , market_metrics.fdmc
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- Usage Data
    , daily_growth.daily_active_addresses as bridge_dau
    , daily_growth.daily_active_addresses as dau
    , daily_growth.cumulative_active_addresses as bridge_cumulative_dau
    , weekly_metrics.weekly_active_addresses as bridge_wau
    , monthly_metrics.monthly_active_addresses as bridge_mau
    , daily_growth.daily_transactions as bridge_txns
    , daily_growth.daily_transactions as txns
    , tvl_metrics.tvl as bridge_tvl
    , tvl_metrics.tvl as tvl
    , daily_growth.daily_volume as bridge_volume
    , daily_growth.daily_volume as volume
    , coalesce(new_addresses.new_addresses, 0) as new_addresses
    , coalesce(returning_addresses.returning_addresses, 0) as returning_addresses
    
    , total_stg_staked_metrics.staked_usd as staked
    , total_stg_staked_metrics.staked_native as staked_native

    -- Fee Data
    , daily_growth.fees as fees
    , daily_growth.supply_side_fee as staking_fee_allocation
    , daily_growth.revenue as token_fee_allocation
    
    -- Financial Statements
    , daily_growth.revenue as revenue
    , daily_growth.token_rewards as token_incentives
    , revenue - token_incentives as earnings
    
    -- Treasury Data
    , treasury_metrics.treasury_usd as treasury
    
    -- Supply Data
    , supply_data.gross_emissions_native
    , supply_data.premine_unlocks_native
    , supply_data.burns_native
    , supply_data.net_supply_change_native
    , supply_data.circulating_supply_native

    -- Token Turnover/Other Data
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- Bespoke Metrics
    , coalesce(b.count_0_100, 0) AS txn_size_0_100
    , coalesce(b.count_100_1K, 0) AS txn_size_100_1k
    , coalesce(b.count_1K_10K, 0) AS txn_size_1k_10k
    , coalesce(b.count_10K_100K, 0) AS txn_size_10k_100k
    , coalesce(b.count_100K_plus, 0) AS txn_size_100k_plus
    , d.avg_daily_transaction_size as avg_txn_size
    , d.daily_active_addresses as bridge_daa
    , market_metrics.price * supply_data.circulating_supply_native as market_cap
    , hydra_metrics.hydra_locked_assets as hydra_tvl

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

FROM treasury_metrics
full outer join daily_growth ON daily_growth.transaction_date = treasury_metrics.date
LEFT JOIN new_addresses ON treasury_metrics.date = new_addresses.transaction_date
LEFT JOIN returning_addresses ON treasury_metrics.date = returning_addresses.transaction_date
LEFT JOIN weekly_metrics ON treasury_metrics.date = DATE(weekly_metrics.week_start)
LEFT JOIN monthly_metrics ON treasury_metrics.date = DATE(monthly_metrics.month_start)
LEFT JOIN transaction_bucket_counts ON treasury_metrics.date = transaction_bucket_counts.transaction_date
LEFT JOIN tvl_metrics ON treasury_metrics.date = tvl_metrics.date
LEFT JOIN total_stg_staked_metrics ON treasury_metrics.date = total_stg_staked_metrics.date
LEFT JOIN supply_data ON treasury_metrics.date = supply_data.date
LEFT JOIN circulating_supply_metrics ON treasury_metrics.date = circulating_supply_metrics.date
LEFT JOIN market_metrics ON treasury_metrics.date = market_metrics.date
LEFT JOIN hydra_metrics ON treasury_metrics.date = hydra_metrics.date
where true
{{ ez_metrics_incremental('treasury_metrics.date', backfill_date) }}
and treasury_metrics.date < to_date(sysdate())
ORDER BY treasury_metrics.date DESC
