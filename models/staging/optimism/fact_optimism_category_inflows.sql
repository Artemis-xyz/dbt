{{
    config(
        materialized="incremental",
        snowflake_warehouse="OPTIMISM",
        unique_key=["chain", "date", "from_app", "to_category", "category"],
    )
}}

{{ get_category_inflows('optimism') }}
