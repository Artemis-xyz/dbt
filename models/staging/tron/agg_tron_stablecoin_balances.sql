{{
    config(
        materialized="incremental",
        unique_key=["address", "contract_address", "date"],
        snowflake_warehouse="BALANCES_LG_2",
    )
}}

{{ agg_foward_filled_stablecoin_balances_by_addresses("tron") }}
