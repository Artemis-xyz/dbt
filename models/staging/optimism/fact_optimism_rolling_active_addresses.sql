{{
    config(
        materialized="incremental",
        unique_key="date",
    )
}}

{{ rolling_active_addresses("optimism", "_v2") }}