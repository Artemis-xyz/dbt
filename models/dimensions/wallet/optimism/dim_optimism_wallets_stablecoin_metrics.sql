{{ config(materialized="table", snowflake_warehouse="OPTIMISM") }}

with wallet_stablecoin_data as ({{ get_wallet_stablecoin_metrics("optimism") }})
select
    address,
    first_stablecoin_to_address,
    first_stablecoin_from_address,
    avg_stablecoin_send,
    avg_stablecoin_received,
    top_stablecoin_to_address,
    top_stablecoin_from_address,
    number_of_stablecoin_transfers_txns,
    number_of_stablecoin_received_txns,
    unique_count_to_address,
    unique_count_from_address,
    first_stablecoin_transfer_date,
    latest_stablecoin_transfer_date,
    first_stablecoin_received_date,
    latest_stablecoin_received_date
from wallet_stablecoin_data
