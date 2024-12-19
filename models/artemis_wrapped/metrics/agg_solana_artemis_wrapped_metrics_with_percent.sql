{{config(materialized='table', snowflake_warehouse='BALANCES_LG')}}
SELECT 
    address,
    total_txns,
    (SELECT COUNT(*) * 1.0 / (SELECT COUNT(*) FROM agg_data) 
     FROM agg_data AS inner_data 
     WHERE inner_data.total_txns > outer_data.total_txns) AS total_txns_percent_rank,
    total_gas_paid,
    (SELECT COUNT(*) * 1.0 / (SELECT COUNT(*) FROM agg_data) 
     FROM agg_data AS inner_data 
     WHERE inner_data.total_gas_paid > outer_data.total_gas_paid) AS total_gas_paid_percent_rank,
    days_onchain,
    (SELECT COUNT(*) * 1.0 / (SELECT COUNT(*) FROM agg_data) 
     FROM agg_data AS inner_data 
     WHERE inner_data.days_onchain > outer_data.days_onchain) AS days_onchain_percent_rank,
    apps_used,
    (SELECT COUNT(*) * 1.0 / (SELECT COUNT(*) FROM agg_data) 
     FROM agg_data AS inner_data 
     WHERE inner_data.apps_used > outer_data.apps_used) AS apps_used_percent_rank
FROM {{ref("agg_solana_artemis_wrapped_metrics")}} AS outer_data
