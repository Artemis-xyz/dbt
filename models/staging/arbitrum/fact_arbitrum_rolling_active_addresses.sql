{{
    config(
        materialized="incremental",
        unique_key=["date"],
    )
}}

{{ rolling_active_addresses("arbitrum", "_v2") }}