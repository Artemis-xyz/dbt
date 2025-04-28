{{
    config(
        materialized="table",
        unique_key="unique_id",
        snowflake_warehouse="SOLANA_XLG",
    )
}}

{{ forward_filled_address_balances(
        artemis_application_id="jupiter"
        , type="lst"
        , chain="solana"
)}}