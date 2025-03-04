{{
    config(
        materialized="table",
        unique_key="unique_id",
        snowflake_warehouse="STARGATE_MD",
    )
}}

{{ forward_filled_address_balances(
    artemis_application_id="stargate",
    type="pool",
    chain="bsc"
)}}