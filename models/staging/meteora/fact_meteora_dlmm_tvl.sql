{{ config(
    materialized="incremental",
    unique_key="unique_id",
    alias="fact_meteora_dlmm_tvl",
    snowflake_warehouse="METEORA",
) }}

{{ forward_filled_address_balances('solana', 'meteora', 'spot_pool')}}
