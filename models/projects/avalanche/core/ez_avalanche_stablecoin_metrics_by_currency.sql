-- depends_on: {{ ref('fact_avalanche_stablecoin_contracts') }}
{{
    config(
        materialized="table",
        database="avalanche",
        schema="core",
        alias="ez_stablecoin_metrics_by_currency",
        snowflake_warehouse="AVALANCHE",
    )
}}

{{ agg_chain_stablecoin_metrics("avalanche") }}
