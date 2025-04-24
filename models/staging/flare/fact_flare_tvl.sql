{{
    config(
        materialized="table",
        snowflake_warehouse="FLARE",
    )
}}

{{
    get_defillama_metrics("flare")
}}