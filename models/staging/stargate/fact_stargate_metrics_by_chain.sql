WITH
    aggregated_data AS (
        SELECT 
            DATE(TRY_CAST(src_timestamp AS TIMESTAMP)) AS transaction_date,
            dst_chain,
            depositor,
            COUNT(*) AS transactions,
            SUM(usd_value) AS volume,
            MIN(TRY_CAST(src_timestamp AS TIMESTAMP)) AS first_seen
        FROM landing_database.prod_landing.raw_stargate_data_dump
        GROUP BY transaction_date, dst_chain, depositor
    ),

    first_seen_global AS (
        SELECT depositor, MIN(first_seen) AS first_seen_date
        FROM aggregated_data
        GROUP BY depositor
    ),

    chain_metrics AS (
        SELECT 
            a.transaction_date,
            a.dst_chain,
            SUM(a.transactions) AS total_transactions,
            SUM(a.volume) AS total_volume,
            COUNT(DISTINCT a.depositor) AS active_addresses,
            COUNT(DISTINCT CASE WHEN f.first_seen_date = a.first_seen THEN a.depositor END) AS new_addresses,
            COUNT(DISTINCT CASE WHEN a.transactions > 1 THEN a.depositor END) AS returning_addresses,
            SUM(a.volume) / NULLIF(SUM(a.transactions), 0) AS avg_transaction_size
        FROM aggregated_data a
        LEFT JOIN first_seen_global f ON a.depositor = f.depositor
        GROUP BY a.transaction_date, a.dst_chain
    )

SELECT 
    transaction_date as date,
    total_transactions,
    total_volume,
    active_addresses,
    new_addresses,
    returning_addresses,
    avg_transaction_size,
    'stargate' as chain
FROM chain_metrics
ORDER BY transaction_date DESC, total_transactions DESC