{{
    config(
        materialized="table",
        snowflake_warehouse="ONDO",
    )
}}

{{ tokenized_treasury_supply_by_date_and_chain("avalanche") }}
