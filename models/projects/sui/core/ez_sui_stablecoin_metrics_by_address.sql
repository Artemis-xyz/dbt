{{
    config(
        materialized="table",
        unique_key="unique_id",
        database="sui",
        schema="core",
        alias="ez_stablecoin_metrics_by_address",
        snowflake_warehouse="STABLECOIN_V2_LG_2",
    )
}}

{% set contract_address = var('contract_address', "") %} 

{{stablecoin_metrics("sui", contract_address) }}