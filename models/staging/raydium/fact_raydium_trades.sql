{{
    config(
        materialized="incremental",
        unique_key="fact_swaps_id",
        snowflake_warehouse="RAYDIUM",
    )
}}
{{fact_solana_dex_swaps('raydium')}}