{{
    config(
        materialized="incremental",
        unique_key="unique_id",
        database="hyperliquid",
        schema="core",
        alias="ez_stablecoin_metrics_by_address_with_labels",
        snowflake_warehouse="STABLECOIN_V2_LG_2",
        cluster_by=["date", "artemis_application_id"]
    )
}}

{% set contract_address = var('contract_address', "") %} 

{{stablecoin_metrics_automatic_labels("hyperevm", contract_address) }}