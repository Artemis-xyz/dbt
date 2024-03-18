-- depends_on: {{ ref('fact_optimism_stablecoin_contracts') }}
{{
    config(
        materialized="table",
        database="optimism",
        schema="core",
        alias="ez_stablecoin_metrics_by_currency",
        snowflake_warehouse="optimism",
    )
}}

{{ agg_chain_stablecoin_metrics("optimism") }}
