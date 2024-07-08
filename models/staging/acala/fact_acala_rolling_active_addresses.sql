-- depends_on: {{ref("fact_acala_uniq_daily_signers")}}
{{
    config(
        materialized="incremental",
        unique_key=["date"],
        snowflake_warehouse="ACALA",
    )
}}

{{ rolling_active_addresses("acala") }}