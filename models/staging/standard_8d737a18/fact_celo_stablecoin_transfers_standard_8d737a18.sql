{{config(materialized="incremental", snowflake_warehouse="STANDARD_8D737A18", unique_key=["transaction_hash", "transfer_index"])}}

with
stablecoin_transfers as (
    {{ standard_8d737a18_stablecoin_transfers('celo') }}
)
select 
    stablecoin_transfers.transaction_timestamp,
    stablecoin_transfers.date_day,
    stablecoin_transfers.block_number,
    stablecoin_transfers.transfer_index,
    transactions.transaction_index as transaction_position,
    stablecoin_transfers.transaction_hash,
    stablecoin_transfers.sender_address,
    stablecoin_transfers.receiver_address,
    stablecoin_transfers.is_mint,
    stablecoin_transfers.is_burn,
    stablecoin_transfers.amount_asset,
    stablecoin_transfers.inflow,
    stablecoin_transfers.transfer_volume,
    stablecoin_transfers.asset_id,
    stablecoin_transfers.asset_symbol,
    stablecoin_transfers.chain_name,
    stablecoin_transfers.chain_id
from stablecoin_transfers
left join {{ ref("fact_celo_transactions") }} transactions
    using (transaction_hash)