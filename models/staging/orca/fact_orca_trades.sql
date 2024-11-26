{{
    config(
        materialized="incremental",
        unique_key="fact_swaps_id",
        snowflake_warehouse="ORCA",
    )
}}
{{fact_solana_dex_swaps('orca')}}