{{
    config(
        materialized="table",
        unique_key="unique_id",
        snowflake_warehouse="STABLECOIN_V2_LG_2",
    )
}}

{% set contract_address = var('contract_address', "") %}  

{{stablecoin_metrics_p2p("sui", contract_address)}}