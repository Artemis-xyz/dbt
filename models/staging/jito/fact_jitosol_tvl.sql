{{
    config(
        materialized="incremental",
        unique_key="unique_id",
        snowflake_warehouse="ANALYTICS_XL",
    )
}}

{{forward_filled_address_balances(
        artemis_application_id="jito"
        , type="lst_pool"
        , chain="solana"
    )}}