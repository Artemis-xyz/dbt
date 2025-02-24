{{
    config(
        materialized="table",
        unique_key="unique_id",
        snowflake_warehouse="MEDIUM",
    )
}}

{{ forward_filled_address_balances(
    artemis_application_id="stargate",
    type="treasury",
    chain="base"
)}}