-- depends_on: {{ ref('fact_base_stablecoin_contracts') }}
{{
    config(
        materialized="table",
        database="base",
        schema="core",
        alias="ez_stablecoin_metrics_by_currency",
        snowflake_warehouse="base",
    )
}}

{{ agg_chain_stablecoin_metrics("base") }}
