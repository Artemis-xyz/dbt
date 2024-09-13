{{ config(materialized="table", snowflake_warehouse="STABLECOIN_V2_LG") }}

{{ stablecoin_breakdown(["chain"], "month") }}