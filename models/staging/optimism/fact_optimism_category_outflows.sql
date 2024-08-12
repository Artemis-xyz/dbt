{{
    config(
        materialized="incremental",
        snowflake_warehouse="OPTIMISM",
        unique_key=["chain", "date", "category", "to_category", "to_app"],
    )
}}

{{ get_category_outflows('optimism') }}
