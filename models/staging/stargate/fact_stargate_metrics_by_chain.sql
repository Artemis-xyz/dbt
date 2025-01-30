WITH
    aggregated_data AS (
    SELECT 
        dst_chain,
        depositor,
        COUNT(*) AS transactions,
        SUM(usd_value) AS volume,
        MIN(TRY_CAST(src_timestamp AS TIMESTAMP)) AS first_seen
    FROM {{source('PROD_LANDING', 'raw_stargate_data_dump')}}
    GROUP BY dst_chain, depositor
),

chain_metrics AS (
    SELECT 
        dst_chain,
        SUM(transactions) AS total_transactions,
        SUM(volume) AS total_volume,
        COUNT(DISTINCT depositor) AS active_addresses,
        COUNT(DISTINCT depositor) AS new_addresses,
        COUNT(DISTINCT CASE WHEN transactions > 1 THEN depositor END) AS returning_addresses,
        total_volume / total_transactions AS avg_transaction_size
    FROM aggregated_data
    GROUP BY dst_chain
)

SELECT 
    dst_chain,
    total_transactions,
    total_volume,
    active_addresses,
    new_addresses,
    returning_addresses,
    avg_transaction_size
FROM chain_metrics
ORDER BY total_transactions DESC