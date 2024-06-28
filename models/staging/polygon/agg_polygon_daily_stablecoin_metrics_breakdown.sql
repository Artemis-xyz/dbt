{{
    config(
        materialized="incremental",
        unique_key=["date", "symbol", "from_address"],
        snowflake_warehouse="BALANCES_LG_2",
    )
}}

{{ agg_daily_stablecoin_metrics_breakdown("polygon") }}
