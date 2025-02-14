{{
    config(
        materialized="incremental",
        unique_key="unique_id",
        database="ton",
        schema="core",
        alias="ez_stablecoin_metrics_by_address",
        snowflake_warehouse="STABLECOIN_V2_LG_2",
    )
}}

{% set contract_address = var('contract_address', "") %} 

{{stablecoin_metrics("ton", contract_address) }}