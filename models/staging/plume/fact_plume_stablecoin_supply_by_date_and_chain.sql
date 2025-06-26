{{
    config(
        materialized="table",
        snowflake_warehouse="RWA",
    )
}}

{{ stablecoin_supply_by_date_and_chain("plume") }}
