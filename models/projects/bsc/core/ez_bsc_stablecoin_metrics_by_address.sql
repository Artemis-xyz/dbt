{{
    config(
        materialized="incremental",
        unique_key="unique_id",
        database="bsc",
        schema="core",
        alias="ez_stablecoin_metrics_by_address",
        snowflake_warehouse="STABLECOIN_V2_LG_2",
    )
}}


{{stablecoin_metrics("bsc")}}