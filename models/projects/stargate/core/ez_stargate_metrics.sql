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
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
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
, price_data as ({{ get_coingecko_metrics("stargate-finance") }})
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
    COALESCE(b.count_0_100, 0) AS TXN_SIZE_0_100
    , COALESCE(b.count_100_1K, 0) AS TXN_SIZE_100_1K
    , COALESCE(b.count_1K_10K, 0) AS TXN_SIZE_1K_10K
    , COALESCE(b.count_10K_100K, 0) AS TXN_SIZE_10K_100K
    , COALESCE(b.count_100K_plus, 0) AS TXN_SIZE_100K_PLUS
    , d.avg_daily_transaction_size as avg_txn_size
    , d.daily_active_addresses as bridge_daa
    
    --Add to BAM
    , COALESCE(n.new_addresses, 0) AS new_addresses
    , COALESCE(r.returning_addresses, 0) AS returning_addresses
    --Standardized Metrics
    , tvl_metrics.tvl
    , d.daily_transactions as bridge_txns
    , d.daily_volume as bridge_volume
    , d.daily_active_addresses as bridge_dau
    , w.weekly_active_addresses as bridge_wau
    , m.monthly_active_addresses as bridge_mau
    , d.cumulative_active_addresses as bridge_cumulative_dau

    , d.fees as fees
    , d.supply_side_fee as staking_fee_allocation
    , d.revenue as token_fee_allocation
    , d.token_rewards as third_party_token_incentives

    , t.treasury_usd as treasury
    , ts.staked_usd as staked
    , ts.staked_native
    
    , pd.price as price
    , pd.price * sd.circulating_supply_native as market_cap
    , h.hydra_locked_assets as hydra_tvl

    -- Supply Data
    , sd.gross_emissions_native
    , sd.premine_unlocks_native
    , sd.burns_native
    , sd.net_supply_change_native
    , sd.circulating_supply_native

    -- Market Data
    , pd.token_volume
    , pd.fdmc
    , pd.token_turnover_circulating
    , pd.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM treasury_metrics t
full outer join daily_growth d ON d.transaction_date = t.date
LEFT JOIN new_addresses n ON t.date = n.transaction_date
LEFT JOIN returning_addresses r ON t.date = r.transaction_date
LEFT JOIN weekly_metrics w ON t.date = DATE(w.week_start)
LEFT JOIN monthly_metrics m ON t.date = DATE(m.month_start)
LEFT JOIN transaction_bucket_counts b ON t.date = b.transaction_date
LEFT JOIN tvl_metrics ON t.date = tvl_metrics.date
LEFT JOIN total_stg_staked_metrics ts ON t.date = ts.date
LEFT JOIN supply_data sd ON t.date = sd.date
LEFT JOIN circulating_supply_metrics cs ON t.date = cs.date
LEFT JOIN price_data pd ON t.date = pd.date
LEFT JOIN hydra_metrics h ON t.date = h.date
where true
{{ ez_metrics_incremental('t.date', backfill_date) }}
and t.date < to_date(sysdate())
ORDER BY t.date DESC
