{{
    config(
        materialized="incremental",
        unique_key=["date", "symbol"],
        database="bsc",
        schema="core",
        alias="ez_stablecoin_metrics_by_currency",
        snowflake_warehouse="STABLECOIN_V2_LG_2",
    )
}}

{{stablecoin_metrics_by_currency("bsc")}}
