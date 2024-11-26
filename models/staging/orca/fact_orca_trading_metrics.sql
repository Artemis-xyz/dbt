{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="ORCA",
    )
}}

{{fact_solana_trading_metrics('orca')}}