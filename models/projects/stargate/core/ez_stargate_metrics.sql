{{
    config(
        materialized="table",
        snowflake_warehouse="STARGATE",
        database="stargate",
        schema="core",
        alias="ez_metrics",
    )
}}

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
        , sum(balance_usd) as treasury_usd
    from treasury_models
    where balance_usd is not null
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
, tvl_metrics as (
    select
        date
        , sum(balance_usd) as tvl
    from tvl_models
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
        LAG(daily_transactions) OVER (ORDER BY transaction_date) AS prev_day_transactions,
        ROUND(100.0 * (daily_transactions - LAG(daily_transactions) OVER (ORDER BY transaction_date)) 
              / NULLIF(LAG(daily_transactions) OVER (ORDER BY transaction_date), 0), 2) AS daily_growth_pct,
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

-- Final output with simplified GROUP BY
SELECT 
    d.transaction_date as date,
    d.daily_transactions as txns,
    d.avg_daily_transaction_size as avg_txn_size,
    d.daily_volume as bridge_volume,
    d.daily_active_addresses as bridge_daa, 
    COALESCE(n.new_addresses, 0) AS new_addresses,
    COALESCE(r.returning_addresses, 0) AS returning_addresses,
    d.cumulative_active_addresses as cumulative_addresses,
    d.daily_growth_pct,
    d.supply_side_fee,
    d.revenue,
    d.fees,
    d.token_rewards,
    w.week_start,
    w.weekly_active_addresses,
    m.month_start,
    m.monthly_active_addresses,
    COALESCE(b.count_0_100, 0) AS TXN_SIZE_0_100,
    COALESCE(b.count_100_1K, 0) AS TXN_SIZE_100_1K,
    COALESCE(b.count_1K_10K, 0) AS TXN_SIZE_1K_10K,
    COALESCE(b.count_10K_100K, 0) AS TXN_SIZE_10K_100K,
    COALESCE(b.count_100K_plus, 0) AS TXN_SIZE_100K_PLUS,
    t.treasury_usd,
    tvl_metrics.tvl
FROM daily_growth d
LEFT JOIN new_addresses n ON d.transaction_date = n.transaction_date
LEFT JOIN returning_addresses r ON d.transaction_date = r.transaction_date
LEFT JOIN weekly_metrics w ON d.transaction_date = DATE(w.week_start)
LEFT JOIN monthly_metrics m ON d.transaction_date = DATE(m.month_start)
LEFT JOIN transaction_bucket_counts b ON d.transaction_date = b.transaction_date
LEFT JOIN treasury_metrics t ON d.transaction_date = t.date
LEFT JOIN tvl_metrics ON d.transaction_date = tvl_metrics.date
where d.transaction_date < to_date(sysdate())
ORDER BY d.transaction_date DESC
