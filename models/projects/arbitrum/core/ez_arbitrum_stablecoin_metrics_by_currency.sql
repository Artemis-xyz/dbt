-- depends_on: {{ ref('fact_arbitrum_stablecoin_contracts') }}
{{
    config(
        materialized="table",
        database="arbitrum",
        schema="core",
        alias="ez_stablecoin_metrics_by_currency",
        snowflake_warehouse="ARBITRUM_MD",
    )
}}

{{ agg_chain_stablecoin_metrics("arbitrum") }}
