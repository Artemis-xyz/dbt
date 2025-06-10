{{
    config(
        materialized="table",
        snowflake_warehouse="RWA",
    )
}}

{{ rwa_supply_by_date_and_chain("plume") }}
