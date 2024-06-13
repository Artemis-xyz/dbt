--depends_on: {{ ref("fact_fantom_uniq_daily_addresses") }}
{{
    config(
        materialized="incremental",
        unique_key=["date"],
        snowflake_warehouse="FANTOM",
    )
}}

{{ rolling_active_addresses("fantom") }}