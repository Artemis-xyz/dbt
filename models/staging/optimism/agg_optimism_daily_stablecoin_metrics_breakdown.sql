{{
    config(
        materialized="incremental",
        unique_key=["date", "contract_address", "from_address"],
        snowflake_warehouse="STABLECOIN_V2_LG_2",
    )
}}

{{ agg_daily_stablecoin_metrics_breakdown("optimism") }}
