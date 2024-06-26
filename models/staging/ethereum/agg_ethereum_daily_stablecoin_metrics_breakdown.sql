{{
    config(
        materialized="incremental",
        unique_key=["date", "symbol", "from_address"],
        snowflake_warehouse="BALANCES_LG",
    )
}}

{{ agg_daily_stablecoin_metrics_breakdown("ethereum", "FLIPSIDE") }}