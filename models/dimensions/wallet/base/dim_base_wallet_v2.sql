{{ 
    config(
        materialized="table",
        snowflake_warehouse="BASE"
    )
}}

select
    coalesce(fundamental.address, stablecoin.address, dex.address) as address,
    fundamental.app_used,
    fundamental.number_of_apps_used,
    fundamental.category_used,
    fundamental.number_of_categories_used,
    fundamental.total_gas_spent_usd,
    fundamental.total_gas_spent_native,
    fundamental.total_txns,
    fundamental.distinct_to_address,
    fundamental.latest_transaction_timestamp,
    fundamental.first_transaction_timestamp,
    fundamental.number_of_days_active,
    fundamental.first_app,
    fundamental.top_app,
    fundamental.top_to_address,
    fundamental.first_native_transfer,
    fundamental.first_native_received,
    fundamental.first_bridge_used,
    fundamental.number_of_bridge_txns,
    fundamental.top_from_address,
    fundamental.first_from_address,
    fundamental.funded_by_wallet_seeder_date,
    dex.number_dex_trades,
    dex.distinct_pools,
    dex.total_dex_volume,
    dex.avg_dex_trade,
    dex.distinct_dex_platforms,
    dex.distinct_token_out,
    dex.max_dex_trade,
    dex.distinct_days_traded as distinct_days_traded_on_dex,
    stablecoin.first_stablecoin_to_address,
    stablecoin.first_stablecoin_from_address,
    stablecoin.avg_stablecoin_send,
    stablecoin.avg_stablecoin_received,
    stablecoin.top_stablecoin_to_address,
    stablecoin.top_stablecoin_from_address,
    stablecoin.number_of_stablecoin_transfers_txns,
    stablecoin.number_of_stablecoin_received_txns,
    stablecoin.unique_count_to_address,
    stablecoin.unique_count_from_address,
    stablecoin.first_stablecoin_transfer_date,
    stablecoin.latest_stablecoin_transfer_date,
    stablecoin.first_stablecoin_received_date,
    stablecoin.latest_stablecoin_received_date
from {{ ref("dim_base_wallets_fundamental_metrics_v2") }} as fundamental
full join
    {{ ref("dim_base_wallets_stablecoin_metrics") }} as stablecoin
    on fundamental.address = stablecoin.address
full join
    {{ ref("dim_base_wallets_dex_trade") }} as dex
    on fundamental.address = dex.address