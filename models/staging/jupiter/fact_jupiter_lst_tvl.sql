{{
    config(
        materialized="incremental",
        unique_key="unique_id",
        snowflake_warehouse="JUPITER",
    )
}}

{{ forward_filled_address_balances(
        artemis_application_id="jupiter"
        , type="lst_pool"
        , chain="solana"
)}}