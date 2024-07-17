-- depends_on: {{ref("fact_acala_uniq_daily_signers")}}
{{
    config(
        materialized="incremental",
        unique_key=["date"],
    )
}}

{{ rolling_active_addresses("acala") }}