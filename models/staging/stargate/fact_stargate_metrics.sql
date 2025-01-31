WITH 
-- First seen date for each address
first_seen AS (
    SELECT 
        depositor, 
        MIN(DATE(TRY_CAST(src_timestamp AS TIMESTAMP))) AS first_seen_date
    FROM {{source('PROD_LANDING', 'raw_stargate_data_dump')}}
    GROUP BY depositor
),

-- New addresses per day (first transaction)
new_addresses AS (
    SELECT 
        first_seen_date AS transaction_date, 
        COUNT(DISTINCT depositor) AS new_addresses
    FROM first_seen
    GROUP BY transaction_date
),

-- Returning addresses per day (transactions AFTER first_seen_date)
returning_addresses AS (
    SELECT 
        DATE(TRY_CAST(t.src_timestamp AS TIMESTAMP)) AS transaction_date,
        COUNT(DISTINCT t.depositor) AS returning_addresses
    FROM {{source('PROD_LANDING', 'raw_stargate_data_dump')}} t
    JOIN first_seen f 
        ON t.depositor = f.depositor
        AND DATE(TRY_CAST(t.src_timestamp AS TIMESTAMP)) > f.first_seen_date
    GROUP BY transaction_date
),

-- Daily metrics
daily_metrics AS (
    SELECT 
        DATE(TRY_CAST(src_timestamp AS TIMESTAMP)) AS transaction_date,
        COUNT(*) AS daily_transactions,
        AVG(usd_value) AS avg_daily_transaction_size,
        SUM(usd_value) AS daily_volume,
        COUNT(DISTINCT depositor) AS daily_active_addresses,
        SUM(daily_active_addresses) OVER (ORDER BY transaction_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
        AS cumulative_active_addresses,
        SUM(treasury_fee) AS daily_protocol_treasury_fee,
        SUM(usd_value * 0.0001) AS daily_vestg_fee,
        SUM(usd_value * 0.0001) AS daily_lp_fee,
        SUM(usd_value * 0.0001) + SUM(usd_value * 0.0001) AS daily_incentive_driven_fee,
        SUM(daily_incentive_driven_fee) OVER (ORDER BY transaction_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
        AS cumulative_incentive_driven_fee,
        SUM(COALESCE(treasury_fee, 0) + (usd_value * 0.0002)) AS daily_total_fee,
        SUM(daily_total_fee) OVER (ORDER BY transaction_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
        AS cumulative_total_fee
    FROM {{source('PROD_LANDING', 'raw_stargate_data_dump')}}
    GROUP BY transaction_date
),

-- Weekly metrics (directly from raw data)
weekly_metrics AS (
    SELECT 
        DATE_TRUNC('week', transaction_date) AS week_start,
        SUM(DISTINCT daily_active_addresses) AS weekly_active_addresses,
        SUM(daily_protocol_treasury_fee) AS weekly_protocol_treasury_fee,
        SUM(daily_vestg_fee) AS weekly_vestg_fee,
        SUM(daily_lp_fee) AS weekly_lp_fee,
        SUM(daily_incentive_driven_fee) AS weekly_incentive_driven_fee,
        SUM(daily_total_fee) AS weekly_total_fee
    FROM daily_metrics
    GROUP BY week_start
),

-- Monthly metrics (directly from raw data)
monthly_metrics AS (
    SELECT 
        DATE_TRUNC('month', transaction_date) AS month_start,
        SUM(DISTINCT daily_active_addresses) AS monthly_active_addresses,
        SUM(daily_protocol_treasury_fee) AS monthly_protocol_treasury_fee,
        SUM(daily_vestg_fee) AS monthly_vestg_fee,
        SUM(daily_lp_fee) AS monthly_lp_fee,
        SUM(daily_incentive_driven_fee) AS monthly_incentive_driven_fee,
        SUM(daily_total_fee) AS monthly_total_fee,
    FROM daily_metrics
    GROUP BY month_start
),

-- Daily growth percentages
daily_growth AS (
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
        daily_protocol_treasury_fee,
        daily_vestg_fee,
        daily_lp_fee,
        daily_incentive_driven_fee,
        cumulative_incentive_driven_fee,
        daily_total_fee,
        cumulative_total_fee
    FROM daily_metrics
),

-- Transaction Bucket Counts (Pre-Aggregated)
transaction_bucket_counts AS (
    SELECT 
        DATE(TRY_CAST(src_timestamp AS TIMESTAMP)) AS transaction_date,
        COUNT(CASE WHEN usd_value < 100 THEN 1 END) AS count_0_100,
        COUNT(CASE WHEN usd_value BETWEEN 100 AND 1000 THEN 1 END) AS count_100_1K,
        COUNT(CASE WHEN usd_value BETWEEN 1000 AND 10000 THEN 1 END) AS count_1K_10K,
        COUNT(CASE WHEN usd_value BETWEEN 10000 AND 100000 THEN 1 END) AS count_10K_100K,
        COUNT(CASE WHEN usd_value >= 100000 THEN 1 END) AS count_100K_plus
    FROM {{source('PROD_LANDING', 'raw_stargate_data_dump')}}
    GROUP BY transaction_date
),

-- Active Address Analytics (Daily, Weekly, Monthly, Cumulative)
active_address_analytics AS (
    SELECT 
        d.transaction_date,
        d.daily_active_addresses,
        
    FROM daily_metrics d
    LEFT JOIN weekly_metrics w ON d.transaction_date = DATE(w.week_start)
    LEFT JOIN monthly_metrics m ON d.transaction_date = DATE(m.month_start)
)

-- Final output with simplified GROUP BY
SELECT 
    d.transaction_date as date,
    d.daily_transactions as txns,
    d.avg_daily_transaction_size as avg_txn_size,
    d.daily_volume as bridge_volume,
    d.daily_active_addresses as dau,
    COALESCE(n.new_addresses, 0) AS new_addresses,
    COALESCE(r.returning_addresses, 0) AS returning_addresses,
    d.cumulative_active_addresses as cumulative_addresses,
    d.daily_growth_pct,
    d.daily_protocol_treasury_fee,
    d.daily_vestg_fee,
    d.daily_lp_fee,
    d.daily_incentive_driven_fee as incentive_driven_fee,
    d.cumulative_incentive_driven_fee as cumulative_incentive_driven_fee,
    d.daily_total_fee as fees,
    d.cumulative_total_fee as cumulative_fees,
    w.week_start,
    w.weekly_active_addresses,
    w.weekly_protocol_treasury_fee,
    w.weekly_vestg_fee,
    w.weekly_lp_fee,
    w.weekly_total_fee,
    m.month_start,
    m.monthly_active_addresses,
    m.monthly_protocol_treasury_fee,
    m.monthly_vestg_fee,
    m.monthly_lp_fee,
    m.monthly_total_fee,
    COALESCE(b.count_0_100, 0) AS txn_size_0_100,
    COALESCE(b.count_100_1K, 0) AS txn_size_100_1k,
    COALESCE(b.count_1K_10K, 0) AS txn_size_1k_10k,
    COALESCE(b.count_10K_100K, 0) AS txn_size_10k_100k,
    COALESCE(b.count_100K_plus, 0) AS txn_size_100k_plus,
    'stargate' as chain
FROM daily_growth d
LEFT JOIN new_addresses n ON d.transaction_date = n.transaction_date
LEFT JOIN returning_addresses r ON d.transaction_date = r.transaction_date
LEFT JOIN weekly_metrics w ON d.transaction_date = DATE(w.week_start)
LEFT JOIN monthly_metrics m ON d.transaction_date = DATE(m.month_start)
LEFT JOIN transaction_bucket_counts b ON d.transaction_date = b.transaction_date
ORDER BY d.transaction_date DESC