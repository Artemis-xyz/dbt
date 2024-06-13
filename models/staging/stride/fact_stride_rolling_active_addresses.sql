--depends_on: {{ ref("fact_stride_uniq_daily_senders") }}
{{
    config(
        materialized="incremental",
        unique_key=["date"],
        snowflake_warehouse="STRIDE",
    )
}}

{{ rolling_active_addresses("stride") }}