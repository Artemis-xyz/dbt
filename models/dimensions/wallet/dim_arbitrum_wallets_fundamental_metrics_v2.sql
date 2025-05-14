{{ 
    config(
        materialized="table", 
        snowflake_warehouse="ARBITRUM_MD"
    ) 
}}

with wallet_fundamental_data as ({{ get_wallet_fundamental_metrics_v2("arbitrum") }})
select
    address,
    app_used,
    number_of_apps_used,
    category_used,
    number_of_categories_used,
    total_gas_spent_usd,
    total_gas_spent_native,
    total_txns,
    distinct_to_address,
    latest_transaction_timestamp,
    first_transaction_timestamp,
    number_of_days_active,
    first_app,
    top_app,
    top_to_address,
    first_native_transfer,
    first_native_received,
    first_bridge_used,
    top_from_address,
    first_from_address,
    funded_by_wallet_seeder_date,
    funded_by_wallet_seeder_tx_hash
from wallet_fundamental_data
