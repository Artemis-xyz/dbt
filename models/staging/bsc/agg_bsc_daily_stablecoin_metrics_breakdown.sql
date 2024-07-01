{{
    config(
        materialized="incremental",
        unique_key=["date", "contract_address", "from_address"],
        snowflake_warehouse="BALANCES_LG",
    )
}}

{{ agg_daily_stablecoin_metrics_breakdown("bsc") }}
